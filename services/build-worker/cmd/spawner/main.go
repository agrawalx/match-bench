// Package main starts the spawner service.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/iicpc/build-worker/internal/consumer"
	k8sspawner "github.com/iicpc/build-worker/internal/k8s"
	"github.com/iicpc/build-worker/internal/publisher"
	"github.com/iicpc/build-worker/internal/store"
	"github.com/iicpc/libs/logger"
	"github.com/iicpc/libs/metrics"
)

// main performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
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
	kafkaBrokers := mustEnv("KAFKA_BROKERS")
	kafkaGroup := envOr("KAFKA_GROUP_ID", "build-worker-spawner")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

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

	minioStore, err := store.NewMinioStore(minioEndpoint, minioAccess, minioSecret, minioBucket, minioSSL)
	if err != nil {
		log.Error("minio init failed", "error", err)
		os.Exit(1)
	}

	pub := publisher.NewKafkaPublisher(kafkaBrokers, log)
	defer pub.Close()

	updater := &statusUpdater{pub: pub, pg: pgStore}

	// Harbor basic-auth creds are only needed for the Harbor registry path; the ECR path
	// authenticates via IRSA (GetAuthorizationToken), so they're optional there.
	registryProvider := os.Getenv("REGISTRY_PROVIDER")
	harborUser, harborPassword := "", ""
	if registryProvider != "ecr" {
		harborUser = mustEnv("HARBOR_USER")
		harborPassword = mustEnv("HARBOR_PASSWORD")
	}

	jobCfg := k8sspawner.JobConfig{
		Namespace:     envOr("K8S_NAMESPACE", "build"),
		BuildNodePool: os.Getenv("BUILD_NODE_POOL"),
		SpawnerImage:  mustEnv("SPAWNER_IMAGE"),
		KanikoImage:   envOr("KANIKO_IMAGE", "gcr.io/kaniko-project/executor:v1.23.2"),
		TrivyImage:    envOr("TRIVY_IMAGE", "aquasec/trivy:0.51.4"),
		SyftImage:     envOr("SYFT_IMAGE", "anchore/syft:v1.4.1"),

		MinioEndpoint:  minioEndpoint,
		MinioAccessKey: minioAccess,
		MinioSecretKey: minioSecret,
		MinioBucket:    minioBucket,
		JobSecretName:  envOr("JOB_SECRET_NAME", "spawner-secret"),

		HarborStagingEndpoint:    mustEnv("HARBOR_STAGING_ENDPOINT"),
		HarborProductionEndpoint: mustEnv("HARBOR_PRODUCTION_ENDPOINT"),
		HarborProject:            envOr("HARBOR_PROJECT", "iicpc"),
		HarborUser:               harborUser,
		HarborPassword:           harborPassword,

		RegistryProvider: registryProvider,
		RegistryInsecure: os.Getenv("REGISTRY_INSECURE") == "true",
	}

	spawner, err := k8sspawner.NewSpawner(jobCfg, minioStore, updater, log)
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

// statusUpdater groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type statusUpdater struct {
	pub *publisher.Publisher
	pg  *store.PostgresStore
}

// PublishStatus applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (u *statusUpdater) PublishStatus(ctx context.Context, submissionID, status, message string) error {
	return u.pub.PublishStatus(ctx, submissionID, status, message)
}

// UpdateDBStatus applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (u *statusUpdater) UpdateDBStatus(ctx context.Context, submissionID, status, message string) error {
	return u.pg.UpdateStatus(ctx, submissionID, status, message)
}

// UpdateImageRef applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (u *statusUpdater) UpdateImageRef(ctx context.Context, submissionID, imageRef string) error {
	return u.pg.UpdateImageRef(ctx, submissionID, imageRef)
}

// envOr performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
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
