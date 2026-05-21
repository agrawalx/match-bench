// runner executes a single pipeline step for one submission.
// RUNNER_MODE selects the step: build | scan | sbom.
// It runs inside the k8s Job container, reads config from env vars, and exits.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/iicpc/build-worker/internal/pipeline"
	"github.com/iicpc/build-worker/internal/store"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(log)

	mode := mustEnv("RUNNER_MODE")
	submissionID := mustEnv("SUBMISSION_ID")
	artifactPath := mustEnv("ARTIFACT_PATH")
	minioEndpoint := mustEnv("MINIO_ENDPOINT")
	minioAccess := mustEnv("MINIO_ACCESS_KEY")
	minioSecret := mustEnv("MINIO_SECRET_KEY")
	minioBucket := envOr("MINIO_BUCKET", "submissions")
	minioSSL := os.Getenv("MINIO_USE_SSL") == "true"
	harborStagingEndpoint := mustEnv("HARBOR_STAGING_ENDPOINT")
	harborProject := envOr("HARBOR_PROJECT", "iicpc")
	harborUser := mustEnv("HARBOR_USER")
	harborPassword := mustEnv("HARBOR_PASSWORD")

	ctx := context.Background()

	minioStore, err := store.NewMinioStore(minioEndpoint, minioAccess, minioSecret, minioBucket, minioSSL)
	if err != nil {
		log.Error("minio init failed", "error", err)
		os.Exit(1)
	}

	runner := pipeline.NewNativeRunner(log, harborStagingEndpoint, harborProject, harborUser, harborPassword)

	log.Info("runner started", "mode", mode, "submission_id", submissionID)

	switch mode {
	case "build":
		if err := runBuild(ctx, log, runner, minioStore, submissionID, artifactPath); err != nil {
			log.Error("build step failed", "error", err)
			os.Exit(1)
		}
	case "scan":
		if err := runScan(ctx, log, runner, minioStore, submissionID, harborStagingEndpoint, harborProject); err != nil {
			log.Error("scan step failed", "error", err)
			os.Exit(1)
		}
	case "sbom":
		if err := runSBOM(ctx, log, runner, minioStore, submissionID, harborStagingEndpoint, harborProject); err != nil {
			log.Error("sbom step failed", "error", err)
			os.Exit(1)
		}
	default:
		log.Error("unknown RUNNER_MODE", "mode", mode)
		os.Exit(1)
	}

	log.Info("runner finished", "mode", mode)
}

func runBuild(ctx context.Context, log *slog.Logger, runner *pipeline.NativeRunner, minio *store.MinioStore, submissionID, artifactPath string) error {
	log.Info("downloading artifact", "path", artifactPath)
	zipData, err := minio.DownloadObject(ctx, artifactPath)
	if err != nil {
		return fmt.Errorf("download artifact: %w", err)
	}

	_, buildLog, err := runner.Build(ctx, submissionID, zipData)

	_ = minio.UploadBytes(ctx,
		fmt.Sprintf("submissions/%s/build.log", submissionID),
		"text/plain", buildLog,
	)
	if err != nil {
		return fmt.Errorf("kaniko build: %w", err)
	}
	log.Info("build complete")
	return nil
}

func runScan(ctx context.Context, log *slog.Logger, runner *pipeline.NativeRunner, minio *store.MinioStore, submissionID, stagingEndpoint, project string) error {
	ref := fmt.Sprintf("%s/%s/%s:latest", stagingEndpoint, project, submissionID)
	log.Info("scanning image", "ref", ref)

	report, err := runner.Scan(ctx, ref)
	if err != nil {
		return fmt.Errorf("trivy: %w", err)
	}
	if err := minio.UploadBytes(ctx,
		fmt.Sprintf("submissions/%s/trivy-report.json", submissionID),
		"application/json", report,
	); err != nil {
		return fmt.Errorf("upload trivy report: %w", err)
	}
	log.Info("scan complete")
	return nil
}

func runSBOM(ctx context.Context, log *slog.Logger, runner *pipeline.NativeRunner, minio *store.MinioStore, submissionID, stagingEndpoint, project string) error {
	ref := fmt.Sprintf("%s/%s/%s:latest", stagingEndpoint, project, submissionID)
	log.Info("generating SBOM", "ref", ref)

	sbom, err := runner.SBOM(ctx, ref)
	if err != nil {
		return fmt.Errorf("syft: %w", err)
	}
	if err := minio.UploadBytes(ctx,
		fmt.Sprintf("submissions/%s/sbom.json", submissionID),
		"application/json", sbom,
	); err != nil {
		return fmt.Errorf("upload sbom: %w", err)
	}
	log.Info("SBOM complete")
	return nil
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
