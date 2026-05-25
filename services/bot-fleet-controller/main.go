// bot-fleet-controller drives benchmark runs end-to-end:
// consumes benchmark.requested, allocates a sandbox slot via sandbox-orchestrator,
// publishes WorkloadSpec to bot-fleet workers, fans in bot.ready signals,
// publishes the barrier, and reports back via benchmark.status.updated.
//
// Single replica, locked in — see CONVENTIONS.md §9.
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
	"github.com/iicpc/schemas/topics"
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
	scenario := scenarioFromEnv()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	st, err := store.New(ctx, dbURL)
	if err != nil {
		log.Error("postgres init failed", "error", err)
		os.Exit(1)
	}
	defer st.Close()

	producer := controller.NewProducer(kafkaBrokers, log)
	defer producer.Close()

	// Synchronous crash recovery: every in-flight run becomes failed
	// before the consumers start. CONVENTIONS.md §9.
	if err := controller.RecoverInFlightRuns(ctx, st, producer, log); err != nil {
		log.Error("startup recovery failed", "error", err)
		os.Exit(1)
	}

	orchClient := orchestrator.NewClient(orchURL)
	sessions := controller.NewSessionManager()
	harbor := controller.HarborConfig{Endpoint: harborEndpoint, Project: harborProject}
	runner := controller.NewRunner(sessions, st, orchClient, producer, scenario, harbor, log)
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
	r.Use(middleware.Recoverer)
	r.Get("/healthz", handler.Healthz(sessions))
	r.Get("/readyz", handler.Readyz(ready))

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
			"worker_count", scenario.WorkerCount,
			"bot_count", scenario.BotCount,
			"orders_per_bot", scenario.OrdersPerBot,
			"run_duration", scenario.RunDuration,
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

// scenarioFromEnv loads the hardcoded v1 scenario, allowing env overrides.
// Replace with a scenarios table lookup when open question #1 in
// BOT_FLEET_PIPELINE.md §9 is resolved.
func scenarioFromEnv() controller.Scenario {
	return controller.Scenario{
		WorkerCount:      uint32(envOrInt("SCENARIO_WORKER_COUNT", 1)),
		BotCount:         uint32(envOrInt("SCENARIO_BOT_COUNT", 10)),
		OrdersPerBot:     uint32(envOrInt("SCENARIO_ORDERS_PER_BOT", 100)),
		GlobalSeed:       uint64(envOrInt("SCENARIO_GLOBAL_SEED", 42)),
		FIXVersion:       envOr("SCENARIO_FIX_VERSION", "FIX.4.2"),
		ConnectTimeoutMS: uint64(envOrInt("SCENARIO_CONNECT_TIMEOUT_MS", 1500)),
		WriteTimeoutMS:   uint64(envOrInt("SCENARIO_WRITE_TIMEOUT_MS", 250)),
		ProfileMix: []topics.BotProfileWeight{
			{Profile: "market_maker", Weight: 40},
			{Profile: "aggressive_taker", Weight: 40},
			{Profile: "canceller", Weight: 20},
		},
		DeployDeadline:   envOrDuration("DEPLOY_DEADLINE", 60*time.Second),
		ReadyDeadline:    envOrDuration("READY_DEADLINE", 30*time.Second),
		BarrierSafetyGap: envOrDuration("BARRIER_SAFETY_GAP", 500*time.Millisecond),
		RunDuration:      envOrDuration("RUN_DURATION", 90*time.Second),
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
