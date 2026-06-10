package pipeline

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"strings"

	"github.com/moby/moby/client"
)

// LocalRunner uses the Docker daemon for build and docker run for trivy/syft.
// Intended for local development only.
type LocalRunner struct {
	log              *slog.Logger
	registryEndpoint string
	registryProject  string
}

func NewLocalRunner(log *slog.Logger) *LocalRunner {
	return &LocalRunner{
		log:              log,
		registryEndpoint: envOr("HARBOR_PRODUCTION_ENDPOINT", "localhost:5000"),
		registryProject:  envOr("HARBOR_PROJECT", "iicpc"),
	}
}

func (r *LocalRunner) Build(ctx context.Context, submissionID string, zipData []byte) (string, []byte, error) {
	imageTag := r.imageRef(submissionID)

	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return "", nil, fmt.Errorf("docker client: %w", err)
	}
	defer cli.Close()

	buildCtx, err := zipToTar(zipData)
	if err != nil {
		return "", nil, fmt.Errorf("prepare build context: %w", err)
	}

	resp, err := cli.ImageBuild(ctx, buildCtx, client.ImageBuildOptions{
		Tags:        []string{imageTag},
		Remove:      true,
		ForceRemove: true,
	})
	if err != nil {
		return "", nil, fmt.Errorf("docker build: %w", err)
	}
	defer resp.Body.Close()

	buildLog, err := streamBuildOutput(resp.Body)
	if err != nil {
		return "", buildLog, fmt.Errorf("build failed: %w", err)
	}
	return imageTag, buildLog, nil
}

func (r *LocalRunner) imageRef(submissionID string) string {
	endpoint := strings.TrimSuffix(r.registryEndpoint, "/")
	project := strings.Trim(strings.TrimSpace(r.registryProject), "/")
	if endpoint == "" || project == "" {
		return fmt.Sprintf("iicpc-%s:latest", submissionID)
	}
	return fmt.Sprintf("%s/%s/%s:latest", endpoint, project, submissionID)
}

func (r *LocalRunner) Scan(ctx context.Context, imageRef string) ([]byte, error) {
	cmd := exec.CommandContext(ctx,
		"docker", "run", "--rm",
		"-v", "/var/run/docker.sock:/var/run/docker.sock",
		"aquasec/trivy:latest",
		"image", "--format", "json", "--exit-code", "0",
		imageRef,
	)
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("trivy: %w", err)
	}
	return out, nil
}

func (r *LocalRunner) SBOM(ctx context.Context, imageRef string) ([]byte, error) {
	cmd := exec.CommandContext(ctx,
		"docker", "run", "--rm",
		"-v", "/var/run/docker.sock:/var/run/docker.sock",
		"anchore/syft:latest",
		imageRef, "-o", "spdx-json",
	)
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("syft: %w", err)
	}
	return out, nil
}

func (r *LocalRunner) Push(ctx context.Context, imageRef string) error {
	if r.registryEndpoint == "" {
		r.log.Info("push step skipped: HARBOR_PRODUCTION_ENDPOINT is empty", "image", imageRef)
		return nil
	}
	cmd := exec.CommandContext(ctx, "docker", "push", imageRef)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("docker push %s: %w: %s", imageRef, err, strings.TrimSpace(string(out)))
	}
	r.log.Info("pushed image", "ref", imageRef)
	return nil
}

func (r *LocalRunner) Cleanup(_ context.Context, _ string) {}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// zipToTar converts a ZIP archive into a tar stream for Docker's build context.
func zipToTar(zipData []byte) (io.Reader, error) {
	zr, err := zip.NewReader(bytes.NewReader(zipData), int64(len(zipData)))
	if err != nil {
		return nil, fmt.Errorf("open zip: %w", err)
	}

	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)

	for _, f := range zr.File {
		rc, err := f.Open()
		if err != nil {
			return nil, err
		}
		data, err := io.ReadAll(rc)
		rc.Close()
		if err != nil {
			return nil, err
		}

		hdr := &tar.Header{Name: f.Name, Mode: 0644, Size: int64(len(data))}
		if f.FileInfo().IsDir() {
			hdr.Name += "/"
			hdr.Typeflag = tar.TypeDir
			hdr.Size = 0
			if err := tw.WriteHeader(hdr); err != nil {
				return nil, err
			}
			continue
		}
		if err := tw.WriteHeader(hdr); err != nil {
			return nil, err
		}
		if _, err := tw.Write(data); err != nil {
			return nil, err
		}
	}
	if err := tw.Close(); err != nil {
		return nil, err
	}
	return &buf, nil
}

// streamBuildOutput reads Docker's JSON build stream and returns the combined log.
func streamBuildOutput(r io.Reader) ([]byte, error) {
	var log bytes.Buffer
	dec := json.NewDecoder(r)
	type msg struct {
		Stream string `json:"stream"`
		Error  string `json:"error"`
	}
	for dec.More() {
		var m msg
		if err := dec.Decode(&m); err != nil {
			break
		}
		if m.Error != "" {
			return log.Bytes(), fmt.Errorf("%s", m.Error)
		}
		log.WriteString(m.Stream)
	}
	return log.Bytes(), nil
}
