package validator

import (
	"archive/zip"
	"bytes"
	"errors"
	"strings"
	"testing"
)

// buildZip creates an in-memory ZIP from a filename→content map.
func buildZip(t *testing.T, files map[string]string) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := zip.NewWriter(&buf)
	for name, content := range files {
		f, err := w.Create(name)
		if err != nil {
			t.Fatalf("buildZip create %s: %v", name, err)
		}
		if _, err := f.Write([]byte(content)); err != nil {
			t.Fatalf("buildZip write %s: %v", name, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("buildZip close: %v", err)
	}
	return buf.Bytes()
}

// base files for each language — tests mutate copies of these.
var goFiles = map[string]string{
	"benchmark.yaml": "protocol: REST\nlanguage: go\nbuild:\n  type: go\n  target: algo\nport: 8080\nteam_name: devtest\n",
	"go.mod":         "module github.com/test/algo\ngo 1.23\n",
	"src/main.go":    "package main\nfunc main() {}\n",
}

var rustFiles = map[string]string{
	"benchmark.yaml": "protocol: REST\nlanguage: rust\nbuild:\n  type: cargo\n  target: algo\nport: 8080\nteam_name: devtest\n",
	"Cargo.toml":     "[package]\nname = \"algo\"\nversion = \"0.1.0\"\n\n[[bin]]\nname = \"algo\"\npath = \"src/main.rs\"\n",
	"src/main.rs":    "fn main() {}\n",
}

var cppFiles = map[string]string{
	"benchmark.yaml": "protocol: REST\nlanguage: cpp\nbuild:\n  type: cmake\n  target: algo\nport: 8080\nteam_name: devtest\n",
	"CMakeLists.txt": "cmake_minimum_required(VERSION 3.20)\nproject(algo)\nadd_executable(algo src/main.cpp)\n",
	"src/main.cpp":   "int main() { return 0; }\n",
}

func copyFiles(m map[string]string) map[string]string {
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// ── Happy paths ──────────────────────────────────────────────────────────────

func TestValidateSubmissionZip_ValidGo(t *testing.T) {
	cfg, err := ValidateSubmissionZip(buildZip(t, goFiles))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Language != "go" {
		t.Errorf("language = %q, want go", cfg.Language)
	}
	if cfg.Build.Target != "algo" {
		t.Errorf("target = %q, want algo", cfg.Build.Target)
	}
	if cfg.Protocol != "REST" {
		t.Errorf("protocol = %q, want REST", cfg.Protocol)
	}
	if cfg.DeclaredPort() != 8080 {
		t.Errorf("port = %d, want 8080", cfg.DeclaredPort())
	}
}

func TestValidateSubmissionZip_ValidRust(t *testing.T) {
	cfg, err := ValidateSubmissionZip(buildZip(t, rustFiles))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Language != "rust" {
		t.Errorf("language = %q, want rust", cfg.Language)
	}
}

func TestValidateSubmissionZip_ValidCpp(t *testing.T) {
	cfg, err := ValidateSubmissionZip(buildZip(t, cppFiles))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Language != "cpp" {
		t.Errorf("language = %q, want cpp", cfg.Language)
	}
}

func TestValidateSubmissionZip_AllProtocols(t *testing.T) {
	for _, proto := range []string{"REST", "FIX", "WS"} {
		t.Run(proto, func(t *testing.T) {
			files := copyFiles(goFiles)
			files["benchmark.yaml"] = strings.ReplaceAll(files["benchmark.yaml"], "REST", proto)
			_, err := ValidateSubmissionZip(buildZip(t, files))
			if err != nil {
				t.Errorf("protocol %s: unexpected error: %v", proto, err)
			}
		})
	}
}

// ── File-level rejections ────────────────────────────────────────────────────

func TestValidateSubmissionZip_NotAZip(t *testing.T) {
	_, err := ValidateSubmissionZip([]byte("this is not a zip file"))
	if !errors.Is(err, ErrNotZip) {
		t.Errorf("got %v, want ErrNotZip", err)
	}
}

func TestValidateSubmissionZip_TooLarge(t *testing.T) {
	data := make([]byte, MaxZipBytes+1)
	_, err := ValidateSubmissionZip(data)
	if !errors.Is(err, ErrTooLarge) {
		t.Errorf("got %v, want ErrTooLarge", err)
	}
}

func TestValidateSubmissionZip_CorruptZip(t *testing.T) {
	// Valid ZIP magic bytes but corrupt body.
	data := []byte{0x50, 0x4B, 0x03, 0x04, 0xFF, 0xFF, 0xFF, 0xFF}
	_, err := ValidateSubmissionZip(data)
	if err == nil {
		t.Error("expected error for corrupt zip, got nil")
	}
}

func TestValidateSubmissionZip_Empty(t *testing.T) {
	_, err := ValidateSubmissionZip([]byte{})
	if !errors.Is(err, ErrNotZip) {
		t.Errorf("got %v, want ErrNotZip", err)
	}
}

// ── Missing required contents ────────────────────────────────────────────────

func TestValidateSubmissionZip_NoBenchmarkYAML(t *testing.T) {
	files := copyFiles(goFiles)
	delete(files, "benchmark.yaml")
	_, err := ValidateSubmissionZip(buildZip(t, files))
	if !errors.Is(err, ErrNoBenchmarkYAML) {
		t.Errorf("got %v, want ErrNoBenchmarkYAML", err)
	}
}

func TestValidateSubmissionZip_NoSrcDir(t *testing.T) {
	files := copyFiles(goFiles)
	delete(files, "src/main.go")
	_, err := ValidateSubmissionZip(buildZip(t, files))
	if !errors.Is(err, ErrNoSrcDir) {
		t.Errorf("got %v, want ErrNoSrcDir", err)
	}
}

// ── benchmark.yaml validation ────────────────────────────────────────────────

func TestValidateSubmissionZip_InvalidYAML(t *testing.T) {
	files := copyFiles(goFiles)
	files["benchmark.yaml"] = ":: not valid yaml ::"
	_, err := ValidateSubmissionZip(buildZip(t, files))
	if err == nil {
		t.Error("expected error for invalid YAML")
	}
}

func TestValidateSubmissionZip_InvalidProtocol(t *testing.T) {
	files := copyFiles(goFiles)
	files["benchmark.yaml"] = strings.ReplaceAll(files["benchmark.yaml"], "REST", "GRPC")
	_, err := ValidateSubmissionZip(buildZip(t, files))
	if err == nil || !strings.Contains(err.Error(), "protocol") {
		t.Errorf("expected protocol error, got %v", err)
	}
}

func TestValidateSubmissionZip_InvalidLanguage(t *testing.T) {
	files := copyFiles(goFiles)
	files["benchmark.yaml"] = strings.ReplaceAll(files["benchmark.yaml"], "go", "python")
	_, err := ValidateSubmissionZip(buildZip(t, files))
	if err == nil || !strings.Contains(err.Error(), "language") {
		t.Errorf("expected language error, got %v", err)
	}
}

func TestValidateSubmissionZip_MissingBuildTarget(t *testing.T) {
	files := copyFiles(goFiles)
	files["benchmark.yaml"] = "protocol: REST\nlanguage: go\nbuild:\n  type: go\n  target: \"\"\nport: 8080\nteam_name: devtest\n"
	_, err := ValidateSubmissionZip(buildZip(t, files))
	if err == nil || !strings.Contains(err.Error(), "build.target") {
		t.Errorf("expected build.target error, got %v", err)
	}
}

func TestValidateSubmissionZip_PortTooLow(t *testing.T) {
	files := copyFiles(goFiles)
	files["benchmark.yaml"] = strings.ReplaceAll(files["benchmark.yaml"], "8080", "80")
	_, err := ValidateSubmissionZip(buildZip(t, files))
	if err == nil || !strings.Contains(err.Error(), "port") {
		t.Errorf("expected port error, got %v", err)
	}
}

func TestValidateSubmissionZip_PortTooHigh(t *testing.T) {
	files := copyFiles(goFiles)
	files["benchmark.yaml"] = strings.ReplaceAll(files["benchmark.yaml"], "8080", "99999")
	_, err := ValidateSubmissionZip(buildZip(t, files))
	if err == nil || !strings.Contains(err.Error(), "port") {
		t.Errorf("expected port error, got %v", err)
	}
}

// ── C++ build target validation ───────────────────────────────────────────────

func TestValidateSubmissionZip_Cpp_WrongBuildType(t *testing.T) {
	files := copyFiles(cppFiles)
	files["benchmark.yaml"] = strings.ReplaceAll(files["benchmark.yaml"], "cmake", "make")
	_, err := ValidateSubmissionZip(buildZip(t, files))
	if err == nil || !strings.Contains(err.Error(), "build.type") {
		t.Errorf("expected build.type error, got %v", err)
	}
}

func TestValidateSubmissionZip_Cpp_MissingCMakeLists(t *testing.T) {
	files := copyFiles(cppFiles)
	delete(files, "CMakeLists.txt")
	_, err := ValidateSubmissionZip(buildZip(t, files))
	if err == nil || !strings.Contains(err.Error(), "CMakeLists.txt") {
		t.Errorf("expected CMakeLists.txt error, got %v", err)
	}
}

func TestValidateSubmissionZip_Cpp_TargetNotInCMake(t *testing.T) {
	files := copyFiles(cppFiles)
	files["CMakeLists.txt"] = "cmake_minimum_required(VERSION 3.20)\nproject(other)\nadd_executable(other src/main.cpp)\n"
	_, err := ValidateSubmissionZip(buildZip(t, files))
	if err == nil || !strings.Contains(err.Error(), "add_executable") {
		t.Errorf("expected add_executable error, got %v", err)
	}
}

// ── Rust build target validation ─────────────────────────────────────────────

func TestValidateSubmissionZip_Rust_WrongBuildType(t *testing.T) {
	files := copyFiles(rustFiles)
	files["benchmark.yaml"] = strings.ReplaceAll(files["benchmark.yaml"], "cargo", "make")
	_, err := ValidateSubmissionZip(buildZip(t, files))
	if err == nil || !strings.Contains(err.Error(), "build.type") {
		t.Errorf("expected build.type error, got %v", err)
	}
}

func TestValidateSubmissionZip_Rust_MissingCargoToml(t *testing.T) {
	files := copyFiles(rustFiles)
	delete(files, "Cargo.toml")
	_, err := ValidateSubmissionZip(buildZip(t, files))
	if err == nil || !strings.Contains(err.Error(), "Cargo.toml") {
		t.Errorf("expected Cargo.toml error, got %v", err)
	}
}

func TestValidateSubmissionZip_Rust_TargetNotInCargo(t *testing.T) {
	files := copyFiles(rustFiles)
	files["Cargo.toml"] = "[package]\nname = \"other\"\nversion = \"0.1.0\"\n\n[[bin]]\nname = \"other\"\npath = \"src/main.rs\"\n"
	_, err := ValidateSubmissionZip(buildZip(t, files))
	if err == nil || !strings.Contains(err.Error(), "[[bin]]") {
		t.Errorf("expected [[bin]] error, got %v", err)
	}
}

// ── Go build target validation ───────────────────────────────────────────────

func TestValidateSubmissionZip_Go_WrongBuildType(t *testing.T) {
	files := copyFiles(goFiles)
	files["benchmark.yaml"] = "protocol: REST\nlanguage: go\nbuild:\n  type: cmake\n  target: algo\nport: 8080\nteam_name: devtest\n"
	_, err := ValidateSubmissionZip(buildZip(t, files))
	if err == nil || !strings.Contains(err.Error(), "build.type") {
		t.Errorf("expected build.type error, got %v", err)
	}
}

func TestValidateSubmissionZip_Go_MissingGoMod(t *testing.T) {
	files := copyFiles(goFiles)
	delete(files, "go.mod")
	_, err := ValidateSubmissionZip(buildZip(t, files))
	if err == nil || !strings.Contains(err.Error(), "go.mod") {
		t.Errorf("expected go.mod error, got %v", err)
	}
}

// ── Internal helpers ─────────────────────────────────────────────────────────

func TestCmakeHasTarget(t *testing.T) {
	cases := []struct {
		name   string
		cmake  string
		target string
		want   bool
	}{
		{"simple", "add_executable(algo src/main.cpp)", "algo", true},
		{"with spaces", "add_executable( algo src/main.cpp )", "algo", true},
		{"multiline", "add_executable(\n  algo\n  src/main.cpp\n)", "algo", true},
		{"wrong target", "add_executable(other src/main.cpp)", "algo", false},
		{"empty", "", "algo", false},
		{"partial match", "add_executable(algo_extra src/main.cpp)", "algo", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := cmakeHasTarget([]byte(c.cmake), c.target)
			if got != c.want {
				t.Errorf("cmakeHasTarget(%q, %q) = %v, want %v", c.cmake, c.target, got, c.want)
			}
		})
	}
}

func TestCargoHasBin(t *testing.T) {
	cases := []struct {
		name   string
		toml   string
		target string
		want   bool
	}{
		{"present", "[[bin]]\nname = \"algo\"\npath = \"src/main.rs\"\n", "algo", true},
		{"absent", "[[bin]]\nname = \"other\"\npath = \"src/main.rs\"\n", "algo", false},
		{"multiple bins first", "[[bin]]\nname = \"algo\"\n\n[[bin]]\nname = \"other\"\n", "algo", true},
		{"multiple bins second", "[[bin]]\nname = \"other\"\n\n[[bin]]\nname = \"algo\"\n", "algo", true},
		{"no bin section", "[package]\nname = \"algo\"\n", "algo", false},
		{"empty", "", "algo", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := cargoHasBin([]byte(c.toml), c.target)
			if got != c.want {
				t.Errorf("cargoHasBin(%q) = %v, want %v", c.target, got, c.want)
			}
		})
	}
}
