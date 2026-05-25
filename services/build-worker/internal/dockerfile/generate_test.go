package dockerfile

import (
	"strings"
	"testing"
)

// ── Happy paths ──────────────────────────────────────────────────────────────

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

// ── Target and port substitution ─────────────────────────────────────────────

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

// ── Multi-stage build structure ───────────────────────────────────────────────

func TestGenerate_MultiStage(t *testing.T) {
	for _, lang := range []string{"go", "rust", "cpp"} {
		t.Run(lang, func(t *testing.T) {
			out, err := Generate(lang, "", "algo", 8080)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			fromCount := strings.Count(out, "\nFROM ")
			// Multi-stage = at least 2 FROM directives.
			// First line starts without \n so add 1 if output starts with FROM.
			if strings.HasPrefix(out, "FROM ") {
				fromCount++
			}
			if fromCount < 2 {
				t.Errorf("lang %s: expected multi-stage (>=2 FROM), got %d", lang, fromCount)
			}
		})
	}
}

// ── Error cases ───────────────────────────────────────────────────────────────

func TestGenerate_UnsupportedLanguage(t *testing.T) {
	_, err := Generate("python", "pip", "app", 8080)
	if err == nil {
		t.Error("expected error for unsupported language, got nil")
	}
	if !strings.Contains(err.Error(), "python") {
		t.Errorf("error should mention the language, got: %v", err)
	}
}

func TestGenerate_EmptyLanguage(t *testing.T) {
	_, err := Generate("", "", "app", 8080)
	if err == nil {
		t.Error("expected error for empty language, got nil")
	}
}

// ── buildType is ignored (reserved) ──────────────────────────────────────────

func TestGenerate_BuildTypeIgnored(t *testing.T) {
	// buildType is currently unused — any value should produce the same output.
	out1, err1 := Generate("go", "go", "algo", 8080)
	out2, err2 := Generate("go", "something_else", "algo", 8080)
	if err1 != nil || err2 != nil {
		t.Fatalf("unexpected errors: %v, %v", err1, err2)
	}
	if out1 != out2 {
		t.Error("buildType should be ignored but produced different output")
	}
}

// ── helper ────────────────────────────────────────────────────────────────────

func assertContains(t *testing.T, haystack, needle string) {
	t.Helper()
	if !strings.Contains(haystack, needle) {
		t.Errorf("expected output to contain %q\nfull output:\n%s", needle, haystack)
	}
}
