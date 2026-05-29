package validator

import (
	"archive/zip"
	"fmt"
	"io"
	"regexp"
	"strings"

	"github.com/BurntSushi/toml"
	cerrs "github.com/iicpc/submission-api/internal/errors"
	"gopkg.in/yaml.v3"
)

const MaxZipBytes = 100 << 20      // 100 MB
const maxRootConfigBytes = 1 << 20 // 1 MB per root config/build file after decompression

var validProtocols = map[string]struct{}{"FIX": {}, "REST": {}, "WS": {}}
var validLanguages = map[string]struct{}{"cpp": {}, "rust": {}, "go": {}}

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

// ValidateSubmissionZip checks magic bytes, size, zip structure, benchmark.yaml,
// src/ presence, build file existence, and target name consistency.
// Root benchmark/build files are read with a 1 MiB decompressed limit so a
// compressed ZIP cannot force unbounded memory allocation while validating.
// Returns a parsed BenchmarkConfig on success, or a descriptive error.
func ValidateSubmissionZip(r io.ReaderAt, size int64) (*BenchmarkConfig, error) {
	if size > MaxZipBytes {
		return nil, cerrs.ErrTooLarge
	}

	// ZIP magic bytes: PK\x03\x04
	var header [4]byte
	if _, err := r.ReadAt(header[:], 0); err != nil {
		return nil, cerrs.ErrNotZip
	}
	if header[0] != 0x50 || header[1] != 0x4B || header[2] != 0x03 || header[3] != 0x04 {
		return nil, cerrs.ErrNotZip
	}

	zr, err := zip.NewReader(r, size)
	if err != nil {
		return nil, cerrs.ErrCorruptZip
	}

	var cfg BenchmarkConfig
	var foundBenchmark, foundSrc bool
	var buildFileContent []byte
	var buildFileName string
	rootEntries := make(map[string]struct{})

	for _, f := range zr.File {
		// Track src/ presence (any entry inside src/ counts)
		if strings.HasPrefix(f.Name, "src/") {
			foundSrc = true
		}

		// Only inspect root-level entries from here on
		if strings.ContainsRune(f.Name, '/') {
			continue
		}
		if _, seen := rootEntries[f.Name]; seen {
			return nil, cerrs.ErrDuplicateRootEntry
		}
		rootEntries[f.Name] = struct{}{}

		switch f.Name {
		case "benchmark.yaml", "benchmark.yml":
			if foundBenchmark {
				return nil, cerrs.ErrMultipleBenchmarkYAML
			}
			rc, err := f.Open()
			if err != nil {
				return nil, fmt.Errorf("open benchmark.yaml: %w", err)
			}
			benchmarkData, readErr := readLimitedRootFile(rc)
			rc.Close()
			if readErr != nil {
				return nil, fmt.Errorf("read benchmark.yaml: %w", readErr)
			}
			if decodeErr := yaml.Unmarshal(benchmarkData, &cfg); decodeErr != nil {
				return nil, fmt.Errorf("invalid benchmark.yaml: %w", decodeErr)
			}
			foundBenchmark = true

		case "CMakeLists.txt", "Cargo.toml", "go.mod":
			if buildFileName != "" {
				return nil, fmt.Errorf("multiple build files at zip root: %s and %s", buildFileName, f.Name)
			}
			rc, err := f.Open()
			if err != nil {
				return nil, fmt.Errorf("open %s: %w", f.Name, err)
			}
			buildFileContent, err = readLimitedRootFile(rc)
			rc.Close()
			if err != nil {
				return nil, fmt.Errorf("read %s: %w", f.Name, err)
			}
			buildFileName = f.Name
		}
	}

	if !foundBenchmark {
		return nil, cerrs.ErrNoBenchmarkYAML
	}
	if !foundSrc {
		return nil, cerrs.ErrNoSrcDir
	}

	if _, ok := validProtocols[cfg.Protocol]; !ok {
		return nil, cerrs.ErrInvalidProtocol
	}
	if _, ok := validLanguages[cfg.Language]; !ok {
		return nil, cerrs.ErrInvalidLanguage
	}
	if cfg.Build.Target == "" {
		return nil, cerrs.ErrMissingBuildTarget
	}
	if cfg.Port < 1024 || cfg.Port > 65535 {
		return nil, cerrs.ErrInvalidPortRange
	}

	if err := validateBuildTarget(&cfg, buildFileName, buildFileContent); err != nil {
		return nil, err
	}

	return &cfg, nil
}

func readLimitedRootFile(r io.Reader) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(r, maxRootConfigBytes+1))
	if err != nil {
		return nil, err
	}
	if len(data) > maxRootConfigBytes {
		return nil, cerrs.ErrRootConfigTooLarge
	}
	return data, nil
}

// validateBuildTarget checks that the declared build.type is consistent with the
// language, the required build file is present, and the target name exists in it.
func validateBuildTarget(cfg *BenchmarkConfig, buildFileName string, buildFileContent []byte) error {
	switch cfg.Language {
	case "cpp":
		if cfg.Build.Type != "cmake" {
			return cerrs.ErrInvalidBuildType
		}
		if buildFileName != "CMakeLists.txt" {
			return cerrs.ErrMissingCMakeLists
		}
		hasTarget, err := cmakeHasTarget(buildFileContent, cfg.Build.Target)
		if err != nil {
			return err
		}
		if !hasTarget {
			return cerrs.ErrMissingCMakeTarget
		}

	case "rust":
		if cfg.Build.Type != "cargo" {
			return cerrs.ErrInvalidBuildType
		}
		if buildFileName != "Cargo.toml" {
			return cerrs.ErrMissingCargoToml
		}
		if !cargoHasBin(buildFileContent, cfg.Build.Target) {
			return cerrs.ErrMissingCargoBin
		}

	case "go":
		if cfg.Build.Type != "go" {
			return cerrs.ErrInvalidBuildType
		}
		if buildFileName != "go.mod" {
			return cerrs.ErrMissingGoMod
		}
		// No target name validation for Go — go build -o {target} ./... accepts any name.
	}

	return nil
}

// cmakeHasTarget checks that CMakeLists.txt contains add_executable(target ...).
// regex is compiled per call because target varies;
// acceptable since this runs once per submission, not on the hot path.
func cmakeHasTarget(data []byte, target string) (bool, error) {
	pattern := `(?im)^\s*add_executable\s*\(\s*` + regexp.QuoteMeta(target) + `[\s),]`
	re, err := regexp.Compile(pattern)
	if err != nil {
		return false, fmt.Errorf("invalid generated CMake target regex: %w", err)
	}
	return re.Match(data), nil
}

// cargoHasBin checks that Cargo.toml has a [[bin]] section with the given name.
func cargoHasBin(data []byte, target string) bool {
	var manifest struct {
		Bin []struct {
			Name string `toml:"name"`
		} `toml:"bin"`
	}
	if err := toml.Unmarshal(data, &manifest); err != nil {
		return false
	}
	for _, bin := range manifest.Bin {
		if bin.Name == target {
			return true
		}
	}
	return false
}
