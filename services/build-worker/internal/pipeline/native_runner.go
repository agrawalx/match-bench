package pipeline

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

const kanikoExecutor = "/kaniko/executor"

// NativeRunner execs kaniko, trivy, and syft binaries directly.
// Intended to run inside the k8s Job container where these binaries are bundled.
// Build pushes directly to Harbor staging; Scan and SBOM read from the staging registry ref.
type NativeRunner struct {
	log                   *slog.Logger
	harborStagingEndpoint string // e.g. harbor-staging.example.com
	harborProject         string // e.g. iicpc
	harborUser            string
	harborPassword        string
}

func NewNativeRunner(log *slog.Logger, harborStagingEndpoint, harborProject, harborUser, harborPassword string) *NativeRunner {
	return &NativeRunner{
		log:                   log,
		harborStagingEndpoint: harborStagingEndpoint,
		harborProject:         harborProject,
		harborUser:            harborUser,
		harborPassword:        harborPassword,
	}
}

func (r *NativeRunner) stagingRef(submissionID string) string {
	return fmt.Sprintf("%s/%s/%s:latest", r.harborStagingEndpoint, r.harborProject, submissionID)
}

// Build extracts zipData, writes Harbor staging credentials for kaniko,
// executes kaniko to build and push to Harbor staging, and returns the staging ref.
func (r *NativeRunner) Build(ctx context.Context, submissionID string, zipData []byte) (string, []byte, error) {
	workDir, err := os.MkdirTemp("", "build-"+submissionID)
	if err != nil {
		return "", nil, fmt.Errorf("mktemp: %w", err)
	}
	defer os.RemoveAll(workDir)

	srcDir := filepath.Join(workDir, "src")
	if err := extractZip(zipData, srcDir); err != nil {
		return "", nil, fmt.Errorf("extract zip: %w", err)
	}

	if err := r.writeKanikoDockerConfig(); err != nil {
		return "", nil, fmt.Errorf("kaniko docker config: %w", err)
	}

	stagingRef := r.stagingRef(submissionID)

	cmd := exec.CommandContext(ctx,
		kanikoExecutor,
		"--context=dir://"+srcDir,
		"--dockerfile="+filepath.Join(srcDir, "Dockerfile"),
		"--destination="+stagingRef,
	)

	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out

	if err := cmd.Run(); err != nil {
		return "", out.Bytes(), fmt.Errorf("kaniko: %w", err)
	}

	r.log.Info("image pushed to staging", "ref", stagingRef)
	return stagingRef, out.Bytes(), nil
}

// Scan runs trivy against the Harbor staging registry image.
func (r *NativeRunner) Scan(ctx context.Context, imageRef string) ([]byte, error) {
	cmd := exec.CommandContext(ctx,
		"trivy", "image",
		"--username", r.harborUser,
		"--password", r.harborPassword,
		"--format", "json",
		"--exit-code", "0",
		imageRef,
	)
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("trivy: %w", err)
	}
	return out, nil
}

// SBOM runs syft against the Harbor staging registry image using env-based auth.
func (r *NativeRunner) SBOM(ctx context.Context, imageRef string) ([]byte, error) {
	// Extract registry hostname for SYFT_REGISTRY_AUTH_AUTHORITY
	authority := strings.SplitN(imageRef, "/", 2)[0]

	cmd := exec.CommandContext(ctx,
		"syft",
		"registry:"+imageRef,
		"-o", "spdx-json",
	)
	cmd.Env = append(os.Environ(),
		"SYFT_REGISTRY_AUTH_AUTHORITY="+authority,
		"SYFT_REGISTRY_AUTH_USERNAME="+r.harborUser,
		"SYFT_REGISTRY_AUTH_PASSWORD="+r.harborPassword,
	)
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("syft: %w", err)
	}
	return out, nil
}

// Push is a no-op — the spawner promotes staging→production via crane.Copy.
func (r *NativeRunner) Push(_ context.Context, _ string) error {
	return nil
}

// Cleanup is a no-op — workDir is removed inside Build().
func (r *NativeRunner) Cleanup(_ context.Context, _ string) {}

// writeKanikoDockerConfig writes Harbor staging credentials to the location
// kaniko reads by default: /kaniko/.docker/config.json.
func (r *NativeRunner) writeKanikoDockerConfig() error {
	type dockerAuth struct {
		Auth string `json:"auth"`
	}
	type dockerConfig struct {
		Auths map[string]dockerAuth `json:"auths"`
	}

	auth := base64.StdEncoding.EncodeToString([]byte(r.harborUser + ":" + r.harborPassword))
	cfg := dockerConfig{
		Auths: map[string]dockerAuth{
			r.harborStagingEndpoint: {Auth: auth},
		},
	}
	data, err := json.Marshal(cfg)
	if err != nil {
		return err
	}
	dir := "/kaniko/.docker"
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, "config.json"), data, 0600)
}

// extractZip extracts zipData into destDir, rejecting any path-traversal entries.
func extractZip(zipData []byte, destDir string) error {
	zr, err := zip.NewReader(bytes.NewReader(zipData), int64(len(zipData)))
	if err != nil {
		return err
	}
	for _, f := range zr.File {
		target := filepath.Join(destDir, f.Name)
		// Reject zip-slip: resolved path must stay inside destDir
		if !strings.HasPrefix(target, filepath.Clean(destDir)+string(filepath.Separator)) {
			return fmt.Errorf("zip-slip: %q escapes destination", f.Name)
		}
		if f.FileInfo().IsDir() {
			os.MkdirAll(target, 0755)
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
