// Package main starts the bot-fleet-controller service.
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
	"strconv"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/iicpc/bot-fleet-controller/internal/controller"
	"github.com/iicpc/bot-fleet-controller/internal/handler"
	"github.com/iicpc/bot-fleet-controller/internal/orchestrator"
	"github.com/iicpc/bot-fleet-controller/internal/store"
	"github.com/iicpc/libs/logger"
	"github.com/iicpc/libs/metrics"
)

// main performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func main() {
	logCfg := logger.DefaultConfig()
	logCfg.ServiceName = "bot-fleet-controller"
	log, lokiClient := logger.NewProductionLogger(logCfg)
	slog.SetDefault(log)
	if lokiClient != nil {
		defer lokiClient.Close()
	}

	port := envOr("PORT", "8080")
	dbURL := mustEnv("DATABASE_URL")
	kafkaBrokers := mustEnv("KAFKA_BROKERS")
	benchmarkGroup := envOr("KAFKA_BENCHMARK_GROUP", "bot-fleet-controller")
	botReadyGroup := envOr("KAFKA_BOT_READY_GROUP", "bot-fleet-controller-ready")
	orchURL := mustEnv("SANDBOX_ORCHESTRATOR_URL")
	runConfig := runConfigFromEnv()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	st, err := store.New(ctx, dbURL)
	if err != nil {
		log.Error("postgres init failed", "error", err)
		os.Exit(1)
	}
	defer st.Close()
	go func() {
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for {
			st.RecordPoolStats()
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()

	producer := controller.NewProducer(kafkaBrokers, log)
	defer producer.Close()

	if err := controller.RecoverInFlightRuns(ctx, st, producer, log); err != nil {
		log.Error("startup recovery failed", "error", err)
		os.Exit(1)
	}

	partitionCtx, partitionCancel := context.WithTimeout(ctx, 30*time.Second)
	workloadPartitions, err := producer.WorkloadPartitionCount(partitionCtx)
	partitionCancel()
	if err != nil {
		log.Error("read workload.assignments partition count failed", "error", err)
		os.Exit(1)
	}
	leases := controller.NewPartitionLeaseAllocator(workloadPartitions)

	maxConcurrentSessions := envOrInt("MAX_CONCURRENT_SESSIONS", 4)

	orchClient := orchestrator.NewClient(orchURL)
	sessions := controller.NewSessionManager()
	runner := controller.NewRunner(sessions, st, orchClient, producer, leases, runConfig, log)
	consumer := controller.NewConsumerWithConcurrency(kafkaBrokers, benchmarkGroup, botReadyGroup, runner, sessions, log, maxConcurrentSessions)
	defer consumer.Close()

	go consumer.StartBenchmarkRequested(ctx)
	go consumer.StartBotReady(ctx)

	ready := handler.NewReadyState()
	ready.MarkReady()

	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(requestLogger(log))
	r.Use(metrics.HTTPMiddleware("bot-fleet-controller", chiRoutePattern))
	r.Use(middleware.Recoverer)
	r.Get("/healthz", handler.Healthz(sessions))
	r.Get("/readyz", handler.Readyz(ready))
	r.Handle("/metrics", metrics.Handler())

	srv := &http.Server{
		Addr:         ":" + port,
		Handler:      r,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	go func() {
		log.Info("bot-fleet-controller started",
			"port", port,
			"orchestrator", orchURL,
			"deploy_deadline", runConfig.DeployDeadline,
			"ready_deadline", runConfig.ReadyDeadline,
			"barrier_safety_gap", runConfig.BarrierSafetyGap,
		)
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
	log.Info("controller stopped")
}

// chiRoutePattern performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func chiRoutePattern(r *http.Request) string {
	if routeCtx := chi.RouteContext(r.Context()); routeCtx != nil {
		return routeCtx.RoutePattern()
	}
	return ""
}

// runConfigFromEnv performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func runConfigFromEnv() controller.RunConfig {
	return controller.RunConfig{
		GlobalSeed:       uint64(envOrInt("GLOBAL_SEED", 42)),
		FIXVersion:       envOr("FIX_VERSION", "FIX.4.2"),
		ConnectTimeoutMS: uint64(envOrInt("CONNECT_TIMEOUT_MS", 1500)),
		WriteTimeoutMS:   uint64(envOrInt("WRITE_TIMEOUT_MS", 250)),

		DeployDeadline:   envOrDuration("DEPLOY_DEADLINE", 60*time.Second),
		ReadyDeadline:    envOrDuration("READY_DEADLINE", 30*time.Second),
		BarrierSafetyGap: envOrDuration("BARRIER_SAFETY_GAP", 500*time.Millisecond),

		MaxTasksPerWorker: envOrInt("MAX_TASKS_PER_WORKER", controller.DefaultMaxTasksPerWorker),

		LeaseAcquireTimeout: envOrDuration("LEASE_ACQUIRE_TIMEOUT", 60*time.Second),
	}
}

// requestLogger performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func requestLogger(log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
			next.ServeHTTP(ww, r)
			log.Info("http request",
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

// envOrInt performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func envOrInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		slog.Warn("invalid integer env, using default", "var", key, "value", v, "default", def)
		return def
	}
	return n
}

// envOrDuration performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func envOrDuration(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		slog.Warn("invalid duration env, using default", "var", key, "value", v, "default", def)
		return def
	}
	return d
}

// mustEnv performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		slog.Error("required env var not set", "var", key)
		os.Exit(1)
	}
	return v
}
