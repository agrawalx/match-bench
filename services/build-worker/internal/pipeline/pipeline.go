// Package pipeline implements pipeline behavior.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package pipeline

import (
	"archive/zip"
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"

	"github.com/iicpc/build-worker/internal/dockerfile"
	"github.com/iicpc/schemas/topics"
)

// StatusUpdater defines the behavior expected by this package boundary.
// Implementations should preserve the caller-visible contract.
type StatusUpdater interface {
	PublishStatus(ctx context.Context, submissionID, status, message string) error
	UpdateDBStatus(ctx context.Context, submissionID, status, message string) error
	UpdateImageRef(ctx context.Context, submissionID, imageRef string) error
}

// MinioClient defines the behavior expected by this package boundary.
// Implementations should preserve the caller-visible contract.
type MinioClient interface {
	DownloadObject(ctx context.Context, objectPath string) ([]byte, error)
	UploadBytes(ctx context.Context, objectPath, contentType string, data []byte) error
}

// Pipeline groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type Pipeline struct {
	minio   MinioClient
	updater StatusUpdater
	runner  StepRunner
	log     *slog.Logger
}

// New performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func New(minio MinioClient, updater StatusUpdater, runner StepRunner, log *slog.Logger) *Pipeline {
	return &Pipeline{minio: minio, updater: updater, runner: runner, log: log}
}

// Run applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (p *Pipeline) Run(ctx context.Context, msg topics.SubmissionBuildRequested) {
	id := msg.SubmissionID
	log := p.log.With("submission_id", id)

	if err := p.run(ctx, msg, log); err != nil {
		log.Error("pipeline failed", "error", err)
		p.setStatus(ctx, id, topics.StatusFailed, err.Error())
	}
}

// run applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (p *Pipeline) run(ctx context.Context, msg topics.SubmissionBuildRequested, log *slog.Logger) error {
	log.Info("downloading artifact", "path", msg.ArtifactPath)
	zipData, err := p.minio.DownloadObject(ctx, msg.ArtifactPath)
	if err != nil {
		return fmt.Errorf("download artifact: %w", err)
	}

	p.setStatus(ctx, msg.SubmissionID, topics.StatusBuilding, "building image")
	log.Info("building image")

	buildZip, err := withGeneratedDockerfile(zipData, msg)
	if err != nil {
		return fmt.Errorf("generate dockerfile: %w", err)
	}
	imageRef, buildLog, err := p.runner.Build(ctx, msg.SubmissionID, buildZip)

	_ = p.minio.UploadBytes(ctx,
		fmt.Sprintf("submissions/%s/build.log", msg.SubmissionID),
		"text/plain", buildLog,
	)
	if err != nil {
		return fmt.Errorf("build: %w", err)
	}
	defer p.runner.Cleanup(ctx, imageRef)
	log.Info("image built", "ref", imageRef)

	log.Info("scanning with trivy")
	report, err := p.runner.Scan(ctx, imageRef)
	if err != nil {
		return fmt.Errorf("trivy scan: %w", err)
	}
	_ = p.minio.UploadBytes(ctx,
		fmt.Sprintf("submissions/%s/trivy-report.json", msg.SubmissionID),
		"application/json", report,
	)
	p.setStatus(ctx, msg.SubmissionID, topics.StatusScanned, "vulnerability scan complete")
	log.Info("trivy scan complete")

	log.Info("generating SBOM with syft")
	sbom, err := p.runner.SBOM(ctx, imageRef)
	if err != nil {
		return fmt.Errorf("syft sbom: %w", err)
	}
	_ = p.minio.UploadBytes(ctx,
		fmt.Sprintf("submissions/%s/sbom.json", msg.SubmissionID),
		"application/json", sbom,
	)
	p.setStatus(ctx, msg.SubmissionID, topics.StatusSBOMReady, "SBOM generated")
	log.Info("SBOM complete")

	if err := p.runner.Push(ctx, imageRef); err != nil {
		return fmt.Errorf("push: %w", err)
	}
	if err := p.updater.UpdateImageRef(ctx, msg.SubmissionID, imageRef); err != nil {
		return fmt.Errorf("persist image ref: %w", err)
	}
	p.setStatus(ctx, msg.SubmissionID, topics.StatusReady, "image ready")
	log.Info("pipeline complete")
	return nil
}

// withGeneratedDockerfile performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func withGeneratedDockerfile(zipData []byte, msg topics.SubmissionBuildRequested) ([]byte, error) {
	content, err := dockerfile.Generate(msg.Language, msg.BuildType, msg.BuildTarget, msg.Port)
	if err != nil {
		return nil, err
	}

	zr, err := zip.NewReader(bytes.NewReader(zipData), int64(len(zipData)))
	if err != nil {
		return nil, fmt.Errorf("open zip: %w", err)
	}

	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	for _, f := range zr.File {
		if f.Name == "Dockerfile" {
			continue
		}
		rc, err := f.Open()
		if err != nil {
			zw.Close()
			return nil, err
		}
		w, err := zw.CreateHeader(&f.FileHeader)
		if err != nil {
			rc.Close()
			zw.Close()
			return nil, err
		}
		_, copyErr := io.Copy(w, rc)
		rc.Close()
		if copyErr != nil {
			zw.Close()
			return nil, copyErr
		}
	}
	w, err := zw.Create("Dockerfile")
	if err != nil {
		zw.Close()
		return nil, err
	}
	if _, err := w.Write([]byte(content)); err != nil {
		zw.Close()
		return nil, err
	}
	if err := zw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// setStatus applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (p *Pipeline) setStatus(ctx context.Context, submissionID, status, message string) {
	if err := p.updater.PublishStatus(ctx, submissionID, status, message); err != nil {
		p.log.Warn("failed to publish status", "status", status, "error", err)
	}
	if err := p.updater.UpdateDBStatus(ctx, submissionID, status, message); err != nil {
		p.log.Warn("failed to update db status", "status", status, "error", err)
	}
}
