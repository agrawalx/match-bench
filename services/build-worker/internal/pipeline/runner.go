// Package pipeline implements runner behavior.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package pipeline

import "context"

// StepRunner defines the behavior expected by this package boundary.
// Implementations should preserve the caller-visible contract.
type StepRunner interface {
	Build(ctx context.Context, submissionID string, zipData []byte) (imageRef string, buildLog []byte, err error)
	Scan(ctx context.Context, imageRef string) ([]byte, error)
	SBOM(ctx context.Context, imageRef string) ([]byte, error)
	Push(ctx context.Context, imageRef string) error
	Cleanup(ctx context.Context, imageRef string)
}
