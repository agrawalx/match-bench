// Package main starts the sandbox-orchestrator service.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
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
	"github.com/iicpc/libs/metrics"
	"github.com/iicpc/sandbox-orchestrator/internal/handler"
	"github.com/iicpc/sandbox-orchestrator/internal/k8s"
	"github.com/iicpc/sandbox-orchestrator/internal/store"
)

// main performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
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
	runtimeClass := os.Getenv("RUNTIME_CLASS")
	algoCPU := envOr("ALGO_CPU", "2")
	algoMemory := envOr("ALGO_MEMORY", "1Gi")
	nodePool := os.Getenv("SANDBOX_NODE_POOL")
	egressBw := os.Getenv("ALGO_EGRESS_BANDWIDTH")
	ingressBw := os.Getenv("ALGO_INGRESS_BANDWIDTH")
	captureEnabled := envOr("CAPTURE_ENABLED", "false") == "true"
	captureImage := os.Getenv("CAPTURE_IMAGE")
	kafkaBrokers := os.Getenv("KAFKA_BROKERS")
	imagePullSecretName := os.Getenv("ALGO_IMAGE_PULL_SECRET")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	k8sClient, err := k8s.NewClient()
	if err != nil {
		log.Error("k8s client init failed", "error", err)
		os.Exit(1)
	}

	mgr, err := k8s.NewManager(k8sClient, k8s.Config{
		Namespace:           namespace,
		RuntimeClass:        runtimeClass,
		CPU:                 algoCPU,
		Memory:              algoMemory,
		NodePool:            nodePool,
		EgressBandwidth:     egressBw,
		IngressBandwidth:    ingressBw,
		CaptureEnabled:      captureEnabled,
		CaptureImage:        captureImage,
		KafkaBrokers:        kafkaBrokers,
		ImagePullSecretName: imagePullSecretName,
	})
	if err != nil {
		log.Error("k8s manager init failed", "error", err)
		os.Exit(1)
	}

	slots := store.NewSlotStore()

	existing, skippedMissingSlotLabel, err := mgr.ListExisting(ctx)
	if err != nil {
		log.Error("list existing slots", "error", err)
		os.Exit(1)
	}
	for i := range existing {
		slots.Put(&existing[i])
	}
	log.Info("rebuilt slot map from cluster", "count", slots.Len(), "namespace", namespace)
	if skippedMissingSlotLabel > 0 {
		log.Warn("skipped managed algo pods without slot label during restore", "count", skippedMissingSlotLabel, "namespace", namespace)
	}

	ready := handler.NewReadyState()

	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(requestLogger(log))
	r.Use(metrics.HTTPMiddleware("sandbox-orchestrator", chiRoutePattern))
	r.Use(middleware.Recoverer)

	r.Get("/healthz", handler.Healthz)
	r.Get("/readyz", handler.Readyz(ready))
	r.Handle("/metrics", metrics.Handler())
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
			"algo_image_pull_secret", secretOrNone(imagePullSecretName),
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

// chiRoutePattern performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func chiRoutePattern(r *http.Request) string {
	if routeCtx := chi.RouteContext(r.Context()); routeCtx != nil {
		return routeCtx.RoutePattern()
	}
	return ""
}

// requestLogger performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
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

// envOr performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// runtimeClassOrNone performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func runtimeClassOrNone(v string) string {
	if v == "" {
		return "(unset — using default runtime)"
	}
	return v
}

// nodePoolOrNone performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func nodePoolOrNone(v string) string {
	if v == "" {
		return "(unset — algo pods schedule on any node)"
	}
	return v
}

// secretOrNone performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func secretOrNone(v string) string {
	if v == "" {
		return "(unset)"
	}
	return v
}
