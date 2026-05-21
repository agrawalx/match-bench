package pipeline

import (
	"archive/zip"
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/crane"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/tarball"
)

const kanikoExecutor = "/kaniko/executor"

// NativeRunner execs kaniko, trivy, and syft binaries directly.
// Intended to run inside the k8s Job container where these binaries are bundled.
type NativeRunner struct {
	log            *slog.Logger
	harborEndpoint string // e.g. harbor.example.com
	harborProject  string // e.g. iicpc
	harborUser     string
	harborPassword string
	submissionID   string // set during Build, used during Push
}

func NewNativeRunner(log *slog.Logger, harborEndpoint, harborProject, harborUser, harborPassword string) *NativeRunner {
	return &NativeRunner{
		log:            log,
		harborEndpoint: harborEndpoint,
		harborProject:  harborProject,
		harborUser:     harborUser,
		harborPassword: harborPassword,
	}
}

func (r *NativeRunner) Build(ctx context.Context, submissionID string, zipData []byte) (string, []byte, error) {
	r.submissionID = submissionID

	workDir, err := os.MkdirTemp("", "build-"+submissionID)
	if err != nil {
		return "", nil, fmt.Errorf("mktemp: %w", err)
	}

	srcDir := filepath.Join(workDir, "src")
	if err := extractZip(zipData, srcDir); err != nil {
		os.RemoveAll(workDir)
		return "", nil, fmt.Errorf("extract zip: %w", err)
	}

	tarPath := filepath.Join(workDir, "image.tar")

	cmd := exec.CommandContext(ctx,
		kanikoExecutor,
		"--context=dir://"+srcDir,
		"--dockerfile="+filepath.Join(srcDir, "Dockerfile"),
		"--no-push",
		"--tar-path="+tarPath,
	)

	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out

	if err := cmd.Run(); err != nil {
		return "", out.Bytes(), fmt.Errorf("kaniko: %w", err)
	}

	return tarPath, out.Bytes(), nil
}

func (r *NativeRunner) Scan(ctx context.Context, imageRef string) ([]byte, error) {
	cmd := exec.CommandContext(ctx,
		"trivy", "image",
		"--input", imageRef,
		"--format", "json",
		"--exit-code", "0",
	)
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("trivy: %w", err)
	}
	return out, nil
}

func (r *NativeRunner) SBOM(ctx context.Context, imageRef string) ([]byte, error) {
	cmd := exec.CommandContext(ctx,
		"syft",
		"oci-archive:"+imageRef,
		"-o", "spdx-json",
	)
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("syft: %w", err)
	}
	return out, nil
}

// Push pushes the verified image tarball to Harbor using crane.
// If HARBOR_ENDPOINT is not configured, the push is skipped (dev/staging mode).
func (r *NativeRunner) Push(ctx context.Context, imageRef string) error {
	if r.harborEndpoint == "" {
		r.log.Info("HARBOR_ENDPOINT not set — skipping push")
		return nil
	}

	dest := fmt.Sprintf("%s/%s/%s:latest", r.harborEndpoint, r.harborProject, r.submissionID)

	ref, err := name.NewTag(dest)
	if err != nil {
		return fmt.Errorf("harbor tag: %w", err)
	}

	img, err := tarball.ImageFromPath(imageRef, nil)
	if err != nil {
		return fmt.Errorf("load tarball: %w", err)
	}

	auth := authn.FromConfig(authn.AuthConfig{
		Username: r.harborUser,
		Password: r.harborPassword,
	})

	if err := crane.Push(img, ref.String(), crane.WithAuth(auth), crane.WithContext(ctx)); err != nil {
		return fmt.Errorf("harbor push: %w", err)
	}

	r.log.Info("image pushed to harbor", "ref", ref.String())
	return nil
}

func (r *NativeRunner) Cleanup(_ context.Context, imageRef string) {
	// imageRef is the tarball path; its parent is the whole work dir
	dir := filepath.Dir(imageRef)
	if dir != "" && dir != "." && dir != "/" {
		os.RemoveAll(dir)
	}
}

func extractZip(zipData []byte, destDir string) error {
	zr, err := zip.NewReader(bytes.NewReader(zipData), int64(len(zipData)))
	if err != nil {
		return err
	}
	for _, f := range zr.File {
		target := filepath.Join(destDir, f.Name)
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
