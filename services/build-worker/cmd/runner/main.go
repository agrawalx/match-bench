// runner executes the build pipeline for a single submission.
// It runs inside the k8s Job container, gets config from env vars, and exits.
package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/iicpc/build-worker/internal/pipeline"
	"github.com/iicpc/build-worker/internal/publisher"
	"github.com/iicpc/build-worker/internal/store"
	"github.com/iicpc/schemas/topics"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(log)

	submissionID := mustEnv("SUBMISSION_ID")
	artifactPath := mustEnv("ARTIFACT_PATH")
	dbURL := mustEnv("DATABASE_URL")
	minioEndpoint := mustEnv("MINIO_ENDPOINT")
	minioAccess := mustEnv("MINIO_ACCESS_KEY")
	minioSecret := mustEnv("MINIO_SECRET_KEY")
	minioBucket := envOr("MINIO_BUCKET", "submissions")
	minioSSL := os.Getenv("MINIO_USE_SSL") == "true"
	kafkaBrokers   := os.Getenv("KAFKA_BROKERS")
	harborEndpoint := os.Getenv("HARBOR_ENDPOINT")
	harborProject  := envOr("HARBOR_PROJECT", "iicpc")
	harborUser     := os.Getenv("HARBOR_USER")
	harborPassword := os.Getenv("HARBOR_PASSWORD")

	ctx := context.Background()

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

	pub := publisher.NewKafkaPublisher(kafkaBrokers, log)
	defer pub.Close()

	updater := &statusUpdater{pub: pub, pg: pgStore}
	runner := pipeline.NewNativeRunner(log, harborEndpoint, harborProject, harborUser, harborPassword)
	pipe := pipeline.New(minioStore, updater, runner, log)

	msg := topics.SubmissionBuildRequested{
		SubmissionID: submissionID,
		ArtifactPath: artifactPath,
		RequestedAt:  time.Now().UTC(),
	}

	log.Info("runner started", "submission_id", submissionID)
	pipe.Run(ctx, msg)
	log.Info("runner finished")
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
