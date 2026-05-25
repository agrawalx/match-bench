// sandbox-orchestrator manages the lifecycle of contestant algorithm pods
// in the sandbox namespace. The bot-fleet-controller calls this service via
// HTTP to allocate, observe, and release one Pod + Service per benchmark run.
//
// Core invariants this service enforces:
//   - slot_id is always the controller's session_id; orchestrator never mints IDs.
//   - One Pod and one Service per slot (both named algo-{slot_id}), created
//     together on POST /slots, deleted together on DELETE /slots/{id}.
//   - Image ref is supplied by the caller (controller composes Harbor refs).
//   - Lazy lifecycle in v1 — no warm pool, no caching. Allocation costs
//     ~3-10 s per run. Warm pool is a v2 optimization.
//   - In-memory slot map; k8s itself is the durable source of truth.
//     On startup we rebuild the map from a Pod list (label app=algo) in
//     the sandbox namespace.
//   - Pod spec is FAIRNESS-driven: Guaranteed QoS, readOnlyRootFilesystem +
//     tmpfs mounts, CNI bandwidth caps, optional gVisor runtime class,
//     optional dedicated node pool. See internal/k8s/slot.go for the
//     full set and the rationale behind each.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/iicpc/libs/logger"
	"github.com/iicpc/sandbox-orchestrator/internal/handler"
	"github.com/iicpc/sandbox-orchestrator/internal/k8s"
	"github.com/iicpc/sandbox-orchestrator/internal/store"
)

func main() {
	logCfg := logger.DefaultConfig()
	logCfg.ServiceName = "sandbox-orchestrator"
	log, lokiClient := logger.NewProductionLogger(logCfg)
	slog.SetDefault(log)
	if lokiClient != nil {
		defer lokiClient.Close()
	}

	port := envOr("PORT", "8080")
	namespace := envOr("K8S_NAMESPACE", "sandbox")
	runtimeClass := os.Getenv("RUNTIME_CLASS") // empty in dev k3s; "gvisor" in prod
	// CPU/memory: same value used for both request AND limit (which is what
	// makes the pod Guaranteed QoS, the precondition for both stable memory
	// accounting and kubelet CPU manager cpuset pinning). CPU MUST be an
	// integer string ("2", not "2000m") for the static-policy CPU manager
	// to allocate a dedicated cpuset; millicore values fall back to shared
	// CFS bandwidth and lose pinning.
	algoCPU := envOr("ALGO_CPU", "2")
	algoMemory := envOr("ALGO_MEMORY", "1Gi")
	// Optional dedicated node pool — mirrors BUILD_NODE_POOL pattern.
	// Empty in dev k3s; "sandbox" in prod (with matching taint on the node).
	nodePool := os.Getenv("SANDBOX_NODE_POOL")
	// Optional per-pod bandwidth caps. CNI bandwidth plugin reads the
	// kubernetes.io/{egress,ingress}-bandwidth annotations.
	egressBw := os.Getenv("ALGO_EGRESS_BANDWIDTH")
	ingressBw := os.Getenv("ALGO_INGRESS_BANDWIDTH")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	k8sClient, err := k8s.NewClient()
	if err != nil {
		log.Error("k8s client init failed", "error", err)
		os.Exit(1)
	}

	mgr := k8s.NewManager(k8sClient, k8s.Config{
		Namespace:        namespace,
		RuntimeClass:     runtimeClass,
		CPU:              algoCPU,
		Memory:           algoMemory,
		NodePool:         nodePool,
		EgressBandwidth:  egressBw,
		IngressBandwidth: ingressBw,
	})

	slots := store.NewSlotStore()

	// Rebuild the in-memory slot map from k8s on every startup.
	//
	// The map is a cache; k8s itself is the durable source of truth. After
	// a crash or rolling restart, the previous in-memory state is gone but
	// any Pod the orchestrator created is still alive in the cluster (or
	// has been cleaned up by its own RestartPolicy=Never failure mode).
	// Listing here lets the new process resume answering GET /slots/{id}
	// for runs that started under the previous process. Any Pod we
	// don't recognise (label app=algo + managed-by=sandbox-orchestrator,
	// but slot label missing) is treated as leaked and ignored — it'll
	// surface as a NotFound on the controller's eventual DELETE.
	existing, err := mgr.ListExisting(ctx)
	if err != nil {
		log.Error("list existing slots", "error", err)
		os.Exit(1)
	}
	for i := range existing {
		slots.Put(&existing[i])
	}
	log.Info("rebuilt slot map from cluster", "count", len(existing), "namespace", namespace)

	ready := handler.NewReadyState()

	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(requestLogger(log))
	r.Use(middleware.Recoverer)

	r.Get("/healthz", handler.Healthz)
	r.Get("/readyz", handler.Readyz(ready))
	r.Post("/slots", handler.CreateSlot(mgr, slots, log))
	r.Get("/slots/{slot_id}", handler.GetSlot(mgr, slots, log))
	r.Delete("/slots/{slot_id}", handler.DeleteSlot(mgr, slots, log))

	srv := &http.Server{
		Addr:         ":" + port,
		Handler:      r,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	go func() {
		log.Info("server started",
			"port", port,
			"runtime_class", runtimeClassOrNone(runtimeClass),
			"algo_cpu", algoCPU,
			"algo_memory", algoMemory,
			"node_pool", nodePoolOrNone(nodePool),
		)
		ready.MarkReady()
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("server error", "error", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	log.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("shutdown error", "error", err)
	}
	log.Info("server stopped")
}

func requestLogger(log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)

			ctx := logger.WithAttrs(r.Context(),
				slog.String("request_id", middleware.GetReqID(r.Context())),
				slog.String("http_method", r.Method),
				slog.String("http_path", r.URL.Path),
			)
			next.ServeHTTP(ww, r.WithContext(ctx))

			log.InfoContext(ctx, "http request completed",
				"method", r.Method,
				"path", r.URL.Path,
				"status", ww.Status(),
				"duration_ms", time.Since(start).Milliseconds(),
			)
		})
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func runtimeClassOrNone(v string) string {
	if v == "" {
		return "(unset — using default runtime)"
	}
	return v
}

func nodePoolOrNone(v string) string {
	if v == "" {
		return "(unset — algo pods schedule on any node)"
	}
	return v
}
