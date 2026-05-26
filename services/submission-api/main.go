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
	"github.com/iicpc/submission-api/internal/consumer"
	"github.com/iicpc/submission-api/internal/handler"
	"github.com/iicpc/submission-api/internal/publisher"
	"github.com/iicpc/submission-api/internal/scenarios"
	"github.com/iicpc/submission-api/internal/store"
)

func main() {
	logCfg := logger.DefaultConfig()
	logCfg.ServiceName = "submission-api"
	log, lokiClient := logger.NewProductionLogger(logCfg)
	slog.SetDefault(log)

	if lokiClient != nil {
		defer lokiClient.Close()
	}

	port := envOr("PORT", "8080")
	dbURL := mustEnv("DATABASE_URL")
	minioEndpoint := mustEnv("MINIO_ENDPOINT")
	minioAccess := mustEnv("MINIO_ACCESS_KEY")
	minioSecret := mustEnv("MINIO_SECRET_KEY")
	minioBucket := envOr("MINIO_BUCKET", "submissions")
	minioSSL := os.Getenv("MINIO_USE_SSL") == "true"
	kafkaBrokers := os.Getenv("KAFKA_BROKERS")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	minioStore, err := store.NewMinioStore(minioEndpoint, minioAccess, minioSecret, minioBucket, minioSSL)
	if err != nil {
		log.Error("minio init failed", "error", err)
		os.Exit(1)
	}

	pgStore, err := store.NewPostgresStore(ctx, dbURL)
	if err != nil {
		log.Error("postgres init failed", "error", err)
		os.Exit(1)
	}
	defer pgStore.Close()

	// Seed the canonical load-test scenarios (constant, spike, ramp) into the
	// scenarios table. The seeder uses INSERT ... ON CONFLICT (name) DO NOTHING,
	// so a judge who tweaked a row by hand will not have their changes
	// overwritten on the next service restart. First boot of a fresh database
	// populates all three; subsequent boots are a no-op.
	scenarioRows, err := scenarios.BuildAll()
	if err != nil {
		log.Error("build scenarios failed", "error", err)
		os.Exit(1)
	}
	storeRows := make([]store.ScenarioRow, len(scenarioRows))
	for i, sr := range scenarioRows {
		storeRows[i] = store.ScenarioRow{
			ScenarioID: sr.ScenarioID,
			Name:       sr.Name,
			SortOrder:  sr.SortOrder,
			DurationNs: sr.DurationNs,
			TaskSpecs:  sr.TaskSpecs,
		}
	}
	if err := pgStore.SeedScenarios(ctx, storeRows); err != nil {
		log.Error("seed scenarios failed", "error", err)
		os.Exit(1)
	}
	log.Info("scenarios seeded", "count", len(storeRows))

	kafkaPub := publisher.NewKafkaPublisher(kafkaBrokers, log)
	defer kafkaPub.Close()

	// Consumer for benchmark.status.updated. This goroutine is the ONLY path
	// by which terminal runs.status values ('completed', 'failed') reach
	// PostgreSQL during normal operation. The only exception platform-wide
	// is the bot-fleet-controller's startup recovery sweep, which writes
	// 'failed' directly to release the partial unique index before its
	// consumers start. Do not add other terminal-status writers — every
	// such addition risks freeing the unique index while resources for the
	// run are still live, defeating the one-active-run-per-submission
	// guarantee that the endpoint's idempotency relies on.
	var benchStatusConsumer *consumer.BenchmarkStatusConsumer
	if kafkaBrokers != "" {
		group := envOr("KAFKA_BENCHMARK_STATUS_GROUP", "submission-api-benchmark-status")
		benchStatusConsumer = consumer.NewBenchmarkStatusConsumer(kafkaBrokers, group, pgStore, log)
		defer benchStatusConsumer.Close()
		go benchStatusConsumer.Start(ctx)
	} else {
		log.Warn("KAFKA_BROKERS not set — benchmark status consumer disabled; runs table will not reflect controller updates")
	}

	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(requestLogger(log))
	r.Use(middleware.Recoverer)

	r.Get("/health", handler.Health(log))
	r.Post("/submit", handler.Submit(minioStore, pgStore, kafkaPub, log))
	r.Get("/submissions/{id}", handler.GetSubmission(pgStore, log))
	// POST /submissions/{id}/benchmark mints one run-group and N child runs
	// (one per row in the scenarios table). The legacy POST /benchmarks/{id}
	// path is kept as an alias so older frontends do not break.
	r.Post("/submissions/{submission_id}/benchmark", handler.StartBenchmark(pgStore, kafkaPub, log))
	r.Post("/benchmarks/{submission_id}", handler.StartBenchmark(pgStore, kafkaPub, log))
	r.Get("/run-groups/{run_group_id}", handler.GetRunGroup(pgStore, log))
	r.Get("/runs/{session_id}", handler.GetRun(pgStore, log))

	srv := &http.Server{
		Addr:         ":" + port,
		Handler:      r,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	go func() {
		log.Info("server started", "port", port)
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
				slog.String("remote_addr", r.RemoteAddr),
				slog.String("user_agent", r.UserAgent()),
			)
			next.ServeHTTP(ww, r.WithContext(ctx))

			log.InfoContext(ctx, "http request completed",
				"method", r.Method,
				"path", r.URL.Path,
				"status", ww.Status(),
				"duration_ms", time.Since(start).Milliseconds(),
				"request_id", middleware.GetReqID(r.Context()),
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

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		slog.Error("required env var not set", "var", key)
		os.Exit(1)
	}
	return v
}
