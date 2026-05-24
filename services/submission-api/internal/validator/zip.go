package validator

import (
	"archive/zip"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

var (
	ErrNotZip          = errors.New("file is not a valid ZIP archive")
	ErrTooLarge        = errors.New("file exceeds 100MB limit")
	ErrNoBenchmarkYAML = errors.New("benchmark.yaml not found at zip root")
	ErrNoSrcDir        = errors.New("src/ directory not found in zip")
)

const MaxZipBytes = 100 << 20 // 100 MB

var validProtocols = map[string]bool{"FIX": true, "REST": true, "WS": true}
var validLanguages = map[string]bool{"cpp": true, "rust": true, "go": true}

// BuildSection is the build stanza from benchmark.yaml.
type BuildSection struct {
	Type   string `yaml:"type"`   // cmake | cargo | go
	Target string `yaml:"target"` // binary name to produce
}

// BenchmarkConfig is the parsed and validated content of benchmark.yaml.
type BenchmarkConfig struct {
	Protocol string       `yaml:"protocol"`
	Language string       `yaml:"language"`
	Build    BuildSection `yaml:"build"`
	Port     int          `yaml:"port"`
	TeamName string       `yaml:"team_name"` // optional until auth is added
}

// DeclaredPort returns the declared port.
func (c *BenchmarkConfig) DeclaredPort() int {
	return c.Port
}

// ValidateSubmissionZip checks magic bytes, size, zip structure, benchmark.yaml,
// src/ presence, build file existence, and target name consistency.
// Returns a parsed BenchmarkConfig on success, or a descriptive error.
func ValidateSubmissionZip(r io.ReaderAt, size int64) (*BenchmarkConfig, error) {
	if size > MaxZipBytes {
		return nil, ErrTooLarge
	}

	// ZIP magic bytes: PK\x03\x04
	var header [4]byte
	if _, err := r.ReadAt(header[:], 0); err != nil {
		return nil, ErrNotZip
	}
	if header[0] != 0x50 || header[1] != 0x4B || header[2] != 0x03 || header[3] != 0x04 {
		return nil, ErrNotZip
	}

	zr, err := zip.NewReader(r, size)
	if err != nil {
		return nil, fmt.Errorf("corrupt zip: %w", err)
	}

	var cfg BenchmarkConfig
	var foundBenchmark, foundSrc bool
	var buildFileContent []byte
	var buildFileName string

	for _, f := range zr.File {
		// Track src/ presence (any entry inside src/ counts)
		if strings.HasPrefix(f.Name, "src/") {
			foundSrc = true
		}

		// Only inspect root-level entries from here on
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

		case "CMakeLists.txt", "Cargo.toml", "go.mod":
			rc, err := f.Open()
			if err != nil {
				return nil, fmt.Errorf("open %s: %w", f.Name, err)
			}
			buildFileContent, err = io.ReadAll(rc)
			rc.Close()
			if err != nil {
				return nil, fmt.Errorf("read %s: %w", f.Name, err)
			}
			buildFileName = f.Name
		}
	}

	if !foundBenchmark {
		return nil, ErrNoBenchmarkYAML
	}
	if !foundSrc {
		return nil, ErrNoSrcDir
	}

	if !validProtocols[cfg.Protocol] {
		return nil, fmt.Errorf("invalid protocol %q: must be FIX, REST, or WS", cfg.Protocol)
	}
	if !validLanguages[cfg.Language] {
		return nil, fmt.Errorf("invalid language %q: must be cpp, rust, or go", cfg.Language)
	}
	if cfg.Build.Target == "" {
		return nil, fmt.Errorf("build.target is required in benchmark.yaml")
	}
	if cfg.Port < 1024 || cfg.Port > 65535 {
		return nil, fmt.Errorf("port %d out of allowed range (1024–65535)", cfg.Port)
	}

	if err := validateBuildTarget(&cfg, buildFileName, buildFileContent); err != nil {
		return nil, err
	}

	return &cfg, nil
}

// validateBuildTarget checks that the declared build.type is consistent with the
// language, the required build file is present, and the target name exists in it.
func validateBuildTarget(cfg *BenchmarkConfig, buildFileName string, buildFileContent []byte) error {
	switch cfg.Language {
	case "cpp":
		if cfg.Build.Type != "cmake" {
			return fmt.Errorf("language cpp requires build.type: cmake, got %q", cfg.Build.Type)
		}
		if buildFileName != "CMakeLists.txt" {
			return errors.New("cpp project must include CMakeLists.txt at zip root")
		}
		if !cmakeHasTarget(buildFileContent, cfg.Build.Target) {
			return fmt.Errorf("CMakeLists.txt has no add_executable target %q", cfg.Build.Target)
		}

	case "rust":
		if cfg.Build.Type != "cargo" {
			return fmt.Errorf("language rust requires build.type: cargo, got %q", cfg.Build.Type)
		}
		if buildFileName != "Cargo.toml" {
			return errors.New("rust project must include Cargo.toml at zip root")
		}
		if !cargoHasBin(buildFileContent, cfg.Build.Target) {
			return fmt.Errorf("Cargo.toml has no [[bin]] with name %q", cfg.Build.Target)
		}

	case "go":
		if cfg.Build.Type != "go" {
			return fmt.Errorf("language go requires build.type: go, got %q", cfg.Build.Type)
		}
		if buildFileName != "go.mod" {
			return errors.New("go project must include go.mod at zip root")
		}
		// No target name validation for Go — go build -o {target} ./... accepts any name.
	}

	return nil
}

// cmakeHasTarget checks that CMakeLists.txt contains add_executable(target ...).
// regex is compiled per call because target varies;
// acceptable since this runs once per submission, not on the hot path.
func cmakeHasTarget(data []byte, target string) bool {
	pattern := `(?im)^\s*add_executable\s*\(\s*` + regexp.QuoteMeta(target) + `[\s),]`
	re, err := regexp.Compile(pattern)
	if err != nil {
		return false
	}
	return re.Match(data)
}

// cargoHasBin checks that Cargo.toml has a [[bin]] section with the given name.
func cargoHasBin(data []byte, target string) bool {
	lines := strings.Split(string(data), "\n")
	inBin := false
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "[[bin]]" {
			inBin = true
			continue
		}
		// Any new section header ends the current [[bin]] block
		if strings.HasPrefix(line, "[") {
			inBin = false
			continue
		}
		if inBin && strings.HasPrefix(line, "name") {
			parts := strings.SplitN(line, "=", 2)
			if len(parts) == 2 {
				name := strings.Trim(strings.TrimSpace(parts[1]), `"`)
				if name == target {
					return true
				}
			}
		}
	}
	return false
}
