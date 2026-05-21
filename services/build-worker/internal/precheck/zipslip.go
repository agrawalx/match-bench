package precheck

import (
	"archive/zip"
	"bytes"
	"fmt"
	"path/filepath"
	"strings"
)

// CheckZipSlip returns an error if any entry in zipData would escape the extract destination.
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
