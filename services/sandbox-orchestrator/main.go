// sandbox-orchestrator manages the lifecycle of contestant algorithm pods
// in the sandbox namespace. The bot-fleet-controller calls this service via
// HTTP to allocate, observe, and release one Pod + Service per benchmark run.
// See CONVENTIONS.md §10 for the invariants this service enforces.
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
	// CPU/memory: same value used for request AND limit (Guaranteed QoS).
	// CPU must be an integer string for cpuset pinning to engage on a
	// kubelet running with cpuManagerPolicy=static. See SANDBOX_FAIRNESS.md.
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

	// Rebuild the in-memory map from k8s. k8s is the durable source of truth
	// (CONVENTIONS.md §10) — anything missing here is a leaked Pod we cannot
	// account for.
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
