// Package precheck defines tests for zipslip test.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package precheck

import (
	"archive/zip"
	"bytes"
	"testing"
)

// buildZip performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func buildZip(t *testing.T, files map[string]string) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := zip.NewWriter(&buf)
	for name, content := range files {
		f, err := w.Create(name)
		if err != nil {
			t.Fatalf("buildZip create %s: %v", name, err)
		}
		f.Write([]byte(content))
	}
	if err := w.Close(); err != nil {
		t.Fatalf("buildZip close: %v", err)
	}
	return buf.Bytes()
}

// buildZipRaw performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func buildZipRaw(t *testing.T, names []string) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := zip.NewWriter(&buf)
	for _, name := range names {
		h := &zip.FileHeader{Name: name, Method: zip.Store}
		f, err := w.CreateHeader(h)
		if err != nil {
			t.Fatalf("buildZipRaw createHeader %s: %v", name, err)
		}
		f.Write([]byte("data"))
	}
	if err := w.Close(); err != nil {
		t.Fatalf("buildZipRaw close: %v", err)
	}
	return buf.Bytes()
}

// TestCheckZipSlip_Clean performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestCheckZipSlip_Clean(t *testing.T) {
	data := buildZip(t, map[string]string{
		"src/main.go":    "package main",
		"go.mod":         "module test",
		"benchmark.yaml": "protocol: REST",
	})
	if err := CheckZipSlip(data); err != nil {
		t.Errorf("unexpected error for clean zip: %v", err)
	}
}

// TestCheckZipSlip_NestedPaths performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestCheckZipSlip_NestedPaths(t *testing.T) {
	data := buildZip(t, map[string]string{
		"src/a/b/c/main.go": "package main",
		"src/utils/util.go": "package utils",
	})
	if err := CheckZipSlip(data); err != nil {
		t.Errorf("unexpected error for nested paths: %v", err)
	}
}

// TestCheckZipSlip_EmptyZip performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestCheckZipSlip_EmptyZip(t *testing.T) {
	data := buildZip(t, map[string]string{})
	if err := CheckZipSlip(data); err != nil {
		t.Errorf("unexpected error for empty zip: %v", err)
	}
}

// TestCheckZipSlip_DotDot performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestCheckZipSlip_DotDot(t *testing.T) {
	data := buildZipRaw(t, []string{"../escape.txt"})
	if err := CheckZipSlip(data); err == nil {
		t.Error("expected error for ../escape.txt, got nil")
	}
}

// TestCheckZipSlip_DotDotNested performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestCheckZipSlip_DotDotNested(t *testing.T) {
	data := buildZipRaw(t, []string{"safe/../../escape.txt"})
	if err := CheckZipSlip(data); err == nil {
		t.Error("expected error for safe/../../escape.txt, got nil")
	}
}

// TestCheckZipSlip_AbsolutePath performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestCheckZipSlip_AbsolutePath(t *testing.T) {
	data := buildZipRaw(t, []string{"/etc/passwd"})
	if err := CheckZipSlip(data); err != nil {
		t.Errorf("absolute path should be safe (filepath.Join absorbs it), got error: %v", err)
	}
}

// TestCheckZipSlip_MixedCleanAndEvil performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestCheckZipSlip_MixedCleanAndEvil(t *testing.T) {
	data := buildZipRaw(t, []string{"src/main.go", "../escape.txt"})
	if err := CheckZipSlip(data); err == nil {
		t.Error("expected error when any entry escapes, got nil")
	}
}

// TestCheckZipSlip_NotAZip performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestCheckZipSlip_NotAZip(t *testing.T) {
	if err := CheckZipSlip([]byte("not a zip")); err == nil {
		t.Error("expected error for non-zip data, got nil")
	}
}

// TestCheckZipSlip_Empty performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestCheckZipSlip_Empty(t *testing.T) {
	if err := CheckZipSlip([]byte{}); err == nil {
		t.Error("expected error for empty input, got nil")
	}
}
