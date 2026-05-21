// spawner is the production entry point.
// It consumes Kafka and creates a k8s Job per submission instead of running the pipeline locally.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/iicpc/build-worker/internal/consumer"
	k8sspawner "github.com/iicpc/build-worker/internal/k8s"
	"github.com/iicpc/build-worker/internal/publisher"
	"github.com/iicpc/build-worker/internal/store"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(log)

	dbURL := mustEnv("DATABASE_URL")
	minioEndpoint := mustEnv("MINIO_ENDPOINT")
	minioAccess := mustEnv("MINIO_ACCESS_KEY")
	minioSecret := mustEnv("MINIO_SECRET_KEY")
	minioBucket := envOr("MINIO_BUCKET", "submissions")
	kafkaBrokers := os.Getenv("KAFKA_BROKERS")
	kafkaGroup := envOr("KAFKA_GROUP_ID", "build-worker-spawner")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	pgStore, err := store.NewPostgresStore(ctx, dbURL)
	if err != nil {
		log.Error("postgres init failed", "error", err)
		os.Exit(1)
	}
	defer pgStore.Close()

	pub := publisher.NewKafkaPublisher(kafkaBrokers, log)
	defer pub.Close()

	updater := &statusUpdater{pub: pub, pg: pgStore}

	jobCfg := k8sspawner.JobConfig{
		Namespace:      envOr("K8S_NAMESPACE", "build"),
		Image:          mustEnv("BUILD_WORKER_IMAGE"),
		DBUrl:          dbURL,
		MinioEndpoint:  minioEndpoint,
		MinioAccessKey: minioAccess,
		MinioSecretKey: minioSecret,
		MinioBucket:    minioBucket,
		KafkaBrokers:   kafkaBrokers,
		HarborEndpoint: os.Getenv("HARBOR_ENDPOINT"),
		HarborProject:  envOr("HARBOR_PROJECT", "iicpc"),
		HarborUser:     os.Getenv("HARBOR_USER"),
		HarborPassword: os.Getenv("HARBOR_PASSWORD"),
	}

	spawner, err := k8sspawner.NewSpawner(jobCfg, updater, log)
	if err != nil {
		log.Error("k8s spawner init failed", "error", err)
		os.Exit(1)
	}

	cons := consumer.NewKafkaConsumer(kafkaBrokers, kafkaGroup, spawner, log)
	defer cons.Close()

	log.Info("build-worker (spawner) started")
	cons.Start(ctx)
	log.Info("spawner stopped")
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
