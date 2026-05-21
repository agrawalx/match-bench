package validator

import (
	"archive/zip"
	"bytes"
	"errors"
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"
)

var (
	ErrNotZip          = errors.New("file is not a valid ZIP archive")
	ErrTooLarge        = errors.New("file exceeds 100MB limit")
	ErrNoBenchmarkYAML = errors.New("benchmark.yaml not found at zip root")
)

const MaxZipBytes = 100 << 20 // 100 MB

var validProtocols = map[string]bool{"FIX": true, "REST": true, "WS": true}
var validLanguages = map[string]bool{"cpp": true, "rust": true, "go": true}

// BenchmarkConfig is the parsed and validated content of benchmark.yaml.
type BenchmarkConfig struct {
	Protocol string `yaml:"protocol"`
	FIXPort  int    `yaml:"fix_port"`
	RESTPort int    `yaml:"rest_port"`
	WSPort   int    `yaml:"ws_port"`
	TeamName string `yaml:"team_name"`
	Language string `yaml:"language"`
}

// DeclaredPort returns the port for the configured protocol.
func (c *BenchmarkConfig) DeclaredPort() int {
	switch c.Protocol {
	case "FIX":
		return c.FIXPort
	case "REST":
		return c.RESTPort
	case "WS":
		return c.WSPort
	}
	return 0
}

// ValidateSubmissionZip checks magic bytes, size, zip structure, and benchmark.yaml.
// data is the full file bytes already read from the upload.
// Returns a parsed BenchmarkConfig on success, or a descriptive error.
func ValidateSubmissionZip(data []byte) (*BenchmarkConfig, error) {
	if len(data) > MaxZipBytes {
		return nil, ErrTooLarge
	}

	// ZIP magic bytes: PK\x03\x04
	if len(data) < 4 || data[0] != 0x50 || data[1] != 0x4B || data[2] != 0x03 || data[3] != 0x04 {
		return nil, ErrNotZip
	}

	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, fmt.Errorf("corrupt zip: %w", err)
	}

	var cfg BenchmarkConfig
	var foundBenchmark bool

	for _, f := range zr.File {
		// Only inspect root-level entries — skip anything with a path separator.
		if strings.ContainsRune(f.Name, '/') {
			continue
		}

		switch f.Name {
		case "benchmark.yaml", "benchmark.yml":
			rc, err := f.Open()
			if err != nil {
				return nil, fmt.Errorf("open benchmark.yaml: %w", err)
			}
			decodeErr := yaml.NewDecoder(rc).Decode(&cfg)
			rc.Close()
			if decodeErr != nil {
				return nil, fmt.Errorf("invalid benchmark.yaml: %w", decodeErr)
			}
			foundBenchmark = true
		}
	}

	if !foundBenchmark {
		return nil, ErrNoBenchmarkYAML
	}

	if !validProtocols[cfg.Protocol] {
		return nil, fmt.Errorf("invalid protocol %q: must be FIX, REST, or WS", cfg.Protocol)
	}
	if !validLanguages[cfg.Language] {
		return nil, fmt.Errorf("invalid language %q: must be cpp, rust, or go", cfg.Language)
	}

	port := cfg.DeclaredPort()
	if port < 1024 || port > 65535 {
		return nil, fmt.Errorf("declared port %d out of allowed range (1024–65535)", port)
	}

	return &cfg, nil
}
