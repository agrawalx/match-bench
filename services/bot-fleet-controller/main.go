// bot-fleet-controller drives benchmark runs end-to-end:
// consumes benchmark.requested, allocates a sandbox slot via sandbox-orchestrator,
// publishes WorkloadSpec to bot-fleet workers, fans in bot.ready signals,
// publishes the barrier, and reports back via benchmark.status.updated.
//
// HARD INVARIANT: this service runs at exactly 1 replica, architecturally locked.
// No sharding, no horizontal scale-out. submission-api is the stateless service
// that scales; the controller stays at 1. Session state lives in-memory behind
// a sync.RWMutex; on crash, in-flight runs are marked failed by the recovery
// sweep on the next startup and the user re-triggers. A PodDisruptionBudget
// with minAvailable=1 protects against routine node drains.
package main

import (
	"context"
	"fmt"
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
	harborEndpoint := mustEnv("HARBOR_PRODUCTION_ENDPOINT")
	harborProject := envOr("HARBOR_PROJECT", "iicpc")
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

	// Synchronous crash recovery — MUST run before consumers start.
	//
	// v1 recovery strategy is "mark-failed-on-restart": every runs row in a
	// non-terminal state (requested|deploying|waiting_ready|barrier_fired|
	// running) is updated to failed with message='controller restart'.
	// User re-triggers via the frontend. No attempt to resume in-flight
	// sessions — their goroutines died with the previous process, the
	// algo pod was likely torn down, and the bot workers have moved on.
	//
	// This must complete BEFORE consumers start because the partial unique
	// index on runs(submission_id) WHERE status NOT IN ('completed','failed')
	// otherwise blocks any retriggered benchmark for an in-flight submission.
	if err := controller.RecoverInFlightRuns(ctx, st, producer, log); err != nil {
		log.Error("startup recovery failed", "error", err)
		os.Exit(1)
	}

	orchClient := orchestrator.NewClient(orchURL)
	sessions := controller.NewSessionManager()
	harbor := controller.HarborConfig{Endpoint: harborEndpoint, Project: harborProject}
	runner := controller.NewRunner(sessions, st, orchClient, producer, runConfig, harbor, log)
	consumer := controller.NewConsumer(kafkaBrokers, benchmarkGroup, botReadyGroup, runner, sessions, log)
	defer consumer.Close()

	go consumer.StartBenchmarkRequested(ctx)
	go consumer.StartBotReady(ctx)

	ready := handler.NewReadyState()
	ready.MarkReady() // consumers are running

	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(requestLogger(log))
	// Problem: the single-replica controller is operationally critical, but
	// only exposed health probes. Fix: export RED metrics and controller domain
	// gauges/counters so alerts can detect stalled or unhealthy coordination.
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
			"harbor", fmt.Sprintf("%s/%s", harborEndpoint, harborProject),
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

func chiRoutePattern(r *http.Request) string {
	if routeCtx := chi.RouteContext(r.Context()); routeCtx != nil {
		return routeCtx.RoutePattern()
	}
	return ""
}

// runConfigFromEnv loads the deployment-wide operational knobs. The
// load-shape (worker count, task list, durations) is now per-scenario and
// lives in the scenarios table — this function only configures the cluster-
// wide deadlines and protocol settings that apply to every benchmark.
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
	}
}

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

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

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

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		slog.Error("required env var not set", "var", key)
		os.Exit(1)
	}
	return v
}
