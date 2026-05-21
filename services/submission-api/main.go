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
	"github.com/iicpc/submission-api/internal/handler"
	"github.com/iicpc/submission-api/internal/logger"
	"github.com/iicpc/submission-api/internal/publisher"
	"github.com/iicpc/submission-api/internal/store"
)

func main() {
	var lokiClient *logger.LokiClient
	var log *slog.Logger

	stdHandler := slog.NewJSONHandler(os.Stdout, nil)
	lokiURL := os.Getenv("LOKI_URL")
	if lokiURL != "" {
		lokiClient = logger.NewLokiClient(lokiURL)
		log = slog.New(logger.NewLokiHandler(lokiClient, stdHandler))
	} else {
		log = slog.New(stdHandler)
	}
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

	kafkaPub := publisher.NewKafkaPublisher(kafkaBrokers, log)
	defer kafkaPub.Close()

	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(requestLogger(log))
	r.Use(middleware.Recoverer)

	r.Get("/health", handler.Health())
	r.Post("/submit", handler.Submit(minioStore, pgStore, kafkaPub, log))
	r.Get("/submissions/{id}", handler.GetSubmission(pgStore, log))

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
			next.ServeHTTP(ww, r)
			log.Info("request",
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
