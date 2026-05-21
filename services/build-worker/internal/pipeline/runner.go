package pipeline

import "context"

// StepRunner abstracts how each pipeline step is executed.
// LocalRunner uses the Docker daemon (dev).
// NativeRunner execs kaniko/trivy/syft binaries (prod, inside k8s Job).
type StepRunner interface {
	// Build builds a container image from zipData.
	// Returns an imageRef — a Docker tag for LocalRunner, a tarball path for NativeRunner.
	Build(ctx context.Context, submissionID string, zipData []byte) (imageRef string, buildLog []byte, err error)
	Scan(ctx context.Context, imageRef string) ([]byte, error)
	SBOM(ctx context.Context, imageRef string) ([]byte, error)
	Push(ctx context.Context, imageRef string) error
	Cleanup(ctx context.Context, imageRef string)
}
