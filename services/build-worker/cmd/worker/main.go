// worker is the local development entry point.
// It consumes Kafka and runs the full pipeline in-process using Docker.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/iicpc/build-worker/internal/consumer"
	"github.com/iicpc/build-worker/internal/pipeline"
	"github.com/iicpc/build-worker/internal/publisher"
	"github.com/iicpc/build-worker/internal/store"
	"github.com/iicpc/libs/logger"
	"github.com/iicpc/libs/metrics"
)

func main() {
	logCfg := logger.DefaultConfig()
	logCfg.ServiceName = "build-worker"
	log, lokiClient := logger.NewProductionLogger(logCfg)
	slog.SetDefault(log)

	if lokiClient != nil {
		defer lokiClient.Close()
	}

	metricsPort := envOr("METRICS_PORT", "9090")
	metricsSrv, err := metrics.StartServer(":" + metricsPort)
	if err != nil {
		log.Error("metrics server start failed", "port", metricsPort, "error", err)
		os.Exit(1)
	}
	defer metricsSrv.Close()
	log.Info("metrics server started", "port", metricsPort)

	dbURL := mustEnv("DATABASE_URL")
	minioEndpoint := mustEnv("MINIO_ENDPOINT")
	minioAccess := mustEnv("MINIO_ACCESS_KEY")
	minioSecret := mustEnv("MINIO_SECRET_KEY")
	minioBucket := envOr("MINIO_BUCKET", "submissions")
	minioSSL := os.Getenv("MINIO_USE_SSL") == "true"
	kafkaBrokers := os.Getenv("KAFKA_BROKERS")
	kafkaGroup := envOr("KAFKA_GROUP_ID", "build-worker")

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
	go func() {
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for {
			pgStore.RecordPoolStats()
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()

	pub := publisher.NewKafkaPublisher(kafkaBrokers, log)
	defer pub.Close()

	updater := &statusUpdater{pub: pub, pg: pgStore}
	runner := pipeline.NewLocalRunner(log)
	pipe := pipeline.New(minioStore, updater, runner, log)

	cons := consumer.NewKafkaConsumer(kafkaBrokers, kafkaGroup, pipe, log)
	defer cons.Close()

	log.Info("build-worker (local) started")
	cons.Start(ctx)
	log.Info("build-worker stopped")
}

type statusUpdater struct {
	pub *publisher.Publisher
	pg  *store.PostgresStore
}

func (u *statusUpdater) PublishStatus(ctx context.Context, submissionID, status, message string) error {
	return u.pub.PublishStatus(ctx, submissionID, status, message)
}

func (u *statusUpdater) UpdateDBStatus(ctx context.Context, submissionID, status, message string) error {
	return u.pg.UpdateStatus(ctx, submissionID, status, message)
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
