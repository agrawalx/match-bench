// Package precheck implements zipslip behavior.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package precheck

import (
	"archive/zip"
	"bytes"
	"fmt"
	"path/filepath"
	"strings"
)

// CheckZipSlip performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func CheckZipSlip(zipData []byte) error {
	zr, err := zip.NewReader(bytes.NewReader(zipData), int64(len(zipData)))
	if err != nil {
		return fmt.Errorf("read zip: %w", err)
	}
	const root = "/safe"
	for _, f := range zr.File {
		target := filepath.Join(root, f.Name)
		if !strings.HasPrefix(target, root+string(filepath.Separator)) && target != root {
			return fmt.Errorf("zip-slip: %q escapes destination", f.Name)
		}
	}
	return nil
}
