// Package main starts the fetcher service.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package main

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/iicpc/build-worker/internal/precheck"
	"github.com/iicpc/build-worker/internal/store"
	"github.com/iicpc/libs/logger"
)

// main performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func main() {
	logCfg := logger.DefaultConfig()
	logCfg.ServiceName = "build-worker-fetcher"
	log, lokiClient := logger.NewProductionLogger(logCfg)
	slog.SetDefault(log)

	if lokiClient != nil {
		defer lokiClient.Close()
	}

	minioEndpoint := mustEnv("MINIO_ENDPOINT")
	minioAccess := mustEnv("MINIO_ACCESS_KEY")
	minioSecret := mustEnv("MINIO_SECRET_KEY")
	minioBucket := envOr("MINIO_BUCKET", "submissions")
	minioSSL := os.Getenv("MINIO_USE_SSL") == "true"
	artifactPath := mustEnv("ARTIFACT_PATH")
	// ECR mode: the kaniko docker-config comes from a mounted Secret (read-only), so the
	// fetcher only fetches the artifact + Dockerfile and does NOT write/require Harbor creds.
	ecrMode := os.Getenv("REGISTRY_PROVIDER") == "ecr"
	var harborEndpoint, harborUser, harborPassword string
	if !ecrMode {
		harborEndpoint = mustEnv("HARBOR_STAGING_ENDPOINT")
		harborUser = mustEnv("HARBOR_USER")
		harborPassword = mustEnv("HARBOR_PASSWORD")
	}

	ctx := context.Background()

	minioStore, err := store.NewMinioStore(minioEndpoint, minioAccess, minioSecret, minioBucket, minioSSL)
	if err != nil {
		log.Error("minio init failed", "error", err)
		os.Exit(1)
	}

	log.Info("downloading artifact", "path", artifactPath)
	zipData, err := minioStore.DownloadObject(ctx, artifactPath)
	if err != nil {
		log.Error("download failed", "error", err)
		os.Exit(1)
	}

	if err := precheck.CheckZipSlip(zipData); err != nil {
		log.Error("zip-slip check failed", "error", err)
		os.Exit(1)
	}

	if err := extractZip(zipData, "/workspace"); err != nil {
		log.Error("extract failed", "error", err)
		os.Exit(1)
	}
	log.Info("extracted to /workspace")

	dockerfileB64 := mustEnv("DOCKERFILE_B64")
	dockerfileData, err := base64.StdEncoding.DecodeString(dockerfileB64)
	if err != nil {
		log.Error("failed to decode dockerfile", "error", err)
		os.Exit(1)
	}
	if err := os.WriteFile("/workspace/Dockerfile", dockerfileData, 0644); err != nil {
		log.Error("failed to write dockerfile", "error", err)
		os.Exit(1)
	}
	log.Info("dockerfile written to /workspace")

	if ecrMode {
		log.Info("ECR mode: skipping kaniko docker config (provided via mounted secret)")
		return
	}
	if err := writeKanikoDockerConfig(harborEndpoint, harborUser, harborPassword); err != nil {
		log.Error("kaniko config failed", "error", err)
		os.Exit(1)
	}
	log.Info("kaniko docker config written")
}

// writeKanikoDockerConfig performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func writeKanikoDockerConfig(endpoint, user, password string) error {
	type dockerAuth struct {
		Auth string `json:"auth"`
	}
	type dockerConfig struct {
		Auths map[string]dockerAuth `json:"auths"`
	}
	auth := base64.StdEncoding.EncodeToString([]byte(user + ":" + password))
	cfg := dockerConfig{Auths: map[string]dockerAuth{endpoint: {Auth: auth}}}
	data, err := json.Marshal(cfg)
	if err != nil {
		return err
	}
	if err := os.MkdirAll("/kaniko/.docker", 0700); err != nil {
		return err
	}
	return os.WriteFile("/kaniko/.docker/config.json", data, 0600)
}

// extractZip performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func extractZip(zipData []byte, destDir string) error {
	zr, err := zip.NewReader(bytes.NewReader(zipData), int64(len(zipData)))
	if err != nil {
		return err
	}
	for _, f := range zr.File {
		target := filepath.Join(destDir, f.Name)
		if f.FileInfo().IsDir() {
			if err := os.MkdirAll(target, 0755); err != nil {
				return err
			}
			continue
		}
		if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
			return err
		}
		rc, err := f.Open()
		if err != nil {
			return err
		}
		data, err := io.ReadAll(rc)
		rc.Close()
		if err != nil {
			return err
		}
		if err := os.WriteFile(target, data, 0644); err != nil {
			return err
		}
	}
	return nil
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
