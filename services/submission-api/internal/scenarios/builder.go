// Package scenarios implements builder behavior.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package scenarios

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/iicpc/schemas/topics"
)

const (
	rpsPerHFT           uint32 = 1000
	rpsPerRetail        uint32 = 5
	rpsPerInstitutional uint32 = 300
)

const (
	mixHFTPct           uint32 = 60
	mixRetailPct        uint32 = 25
	mixInstitutionalPct uint32 = 15
)

const (
	hftMarketPct  uint8 = 10
	hftCancelPct  uint8 = 30
	hftReplacePct uint8 = 10

	retailMarketPct  uint8 = 65
	retailCancelPct  uint8 = 5
	retailReplacePct uint8 = 0

	institutionalMarketPct  uint8 = 20
	institutionalCancelPct  uint8 = 0
	institutionalReplacePct uint8 = 0
)

const (
	rampWaveCount   = 9 // 9 waves; peak ≈ RampPeakRPS held at the top of the ramp
	rampWaveCadence = 20 * time.Second
)

// Config groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type Config struct {
	ConstantDuration time.Duration
	SpikeDuration    time.Duration
	RampDuration     time.Duration

	ConstantTotalRPS uint32
	SpikePeakRPS     uint32
	RampPeakRPS      uint32

	SpikePreWindow   time.Duration
	SpikeBurstWindow time.Duration
}

// DefaultConfig performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func DefaultConfig() Config {
	return Config{
		ConstantDuration: 60 * time.Second,
		SpikeDuration:    60 * time.Second,
		RampDuration:     180 * time.Second,
		ConstantTotalRPS: 10000,
		SpikePeakRPS:     50000,
		RampPeakRPS:      90000,
		SpikePreWindow:   25 * time.Second,
		SpikeBurstWindow: 10 * time.Second,
	}
}

// ConfigFromEnv performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func ConfigFromEnv() Config {
	c := DefaultConfig()
	c.ConstantDuration = envSeconds("CONSTANT_DURATION_S", c.ConstantDuration)
	c.SpikeDuration = envSeconds("SPIKE_DURATION_S", c.SpikeDuration)
	c.RampDuration = envSeconds("RAMP_DURATION_S", c.RampDuration)
	c.ConstantTotalRPS = envUint32("CONSTANT_TOTAL_RPS", c.ConstantTotalRPS)
	c.SpikePeakRPS = envUint32("SPIKE_PEAK_RPS", c.SpikePeakRPS)
	c.RampPeakRPS = envUint32("RAMP_PEAK_RPS", c.RampPeakRPS)
	c.SpikePreWindow = envSeconds("SPIKE_PREWINDOW_S", c.SpikePreWindow)
	c.SpikeBurstWindow = envSeconds("SPIKE_BURST_S", c.SpikeBurstWindow)
	return c
}

// validate applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (c Config) validate() error {
	if c.ConstantDuration <= 0 || c.SpikeDuration <= 0 || c.RampDuration <= 0 {
		return fmt.Errorf("scenario durations must be positive (constant=%v spike=%v ramp=%v)",
			c.ConstantDuration, c.SpikeDuration, c.RampDuration)
	}
	if c.ConstantTotalRPS == 0 || c.SpikePeakRPS == 0 || c.RampPeakRPS == 0 {
		return fmt.Errorf("scenario RPS budgets must be positive (constant=%d spikePeak=%d rampPeak=%d)",
			c.ConstantTotalRPS, c.SpikePeakRPS, c.RampPeakRPS)
	}
	if c.SpikePeakRPS < c.ConstantTotalRPS {
		return fmt.Errorf("spike peak RPS %d must be >= baseline RPS %d", c.SpikePeakRPS, c.ConstantTotalRPS)
	}
	if c.SpikePreWindow+c.SpikeBurstWindow > c.SpikeDuration {
		return fmt.Errorf("spike pre-window %v + burst %v exceed spike duration %v",
			c.SpikePreWindow, c.SpikeBurstWindow, c.SpikeDuration)
	}
	if c.RampDuration <= time.Duration(rampWaveCount-1)*rampWaveCadence {
		return fmt.Errorf("ramp duration %v too short for %d waves at %v cadence",
			c.RampDuration, rampWaveCount, rampWaveCadence)
	}
	return nil
}

// BuildAll performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func BuildAll(cfg Config) ([]ScenarioRow, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	constID, err := newUUID()
	if err != nil {
		return nil, err
	}
	spikeID, err := newUUID()
	if err != nil {
		return nil, err
	}
	rampID, err := newUUID()
	if err != nil {
		return nil, err
	}

	return []ScenarioRow{
		{
			ScenarioID: constID,
			Name:       "constant",
			SortOrder:  1,
			DurationNs: uint64(cfg.ConstantDuration.Nanoseconds()),
			TaskSpecs:  buildConstantTasks(0, 0, cfg.ConstantDuration, cfg.ConstantTotalRPS),
		},
		{
			ScenarioID: spikeID,
			Name:       "spike",
			SortOrder:  2,
			DurationNs: uint64(cfg.SpikeDuration.Nanoseconds()),
			TaskSpecs:  buildSpikeTasks(cfg),
		},
		{
			ScenarioID: rampID,
			Name:       "ramp",
			SortOrder:  3,
			DurationNs: uint64(cfg.RampDuration.Nanoseconds()),
			TaskSpecs:  buildRampTasks(cfg),
		},
	}, nil
}

// ScenarioRow groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type ScenarioRow struct {
	ScenarioID string
	Name       string
	SortOrder  int
	DurationNs uint64
	TaskSpecs  []topics.TaskSpec
}

// botCountsForBudget performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func botCountsForBudget(totalRPS uint32) (hft, retail, inst, realisedRPS uint32) {
	hft = (totalRPS * mixHFTPct / 100) / rpsPerHFT
	retail = (totalRPS * mixRetailPct / 100) / rpsPerRetail
	inst = (totalRPS * mixInstitutionalPct / 100) / rpsPerInstitutional
	realisedRPS = hft*rpsPerHFT + retail*rpsPerRetail + inst*rpsPerInstitutional
	return hft, retail, inst, realisedRPS
}

// buildConstantTasks performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func buildConstantTasks(startTaskID uint32, startOffset, duration time.Duration, totalRPS uint32) []topics.TaskSpec {
	hft, retail, inst, _ := botCountsForBudget(totalRPS)
	return buildLayer(startTaskID, startOffset, duration, hft, retail, inst)
}

// buildSpikeTasks performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func buildSpikeTasks(cfg Config) []topics.TaskSpec {
	baseline := buildConstantTasks(0, 0, cfg.SpikeDuration, cfg.ConstantTotalRPS)

	extraHFT, extraRetail, extraInst, _ := botCountsForBudget(cfg.SpikePeakRPS - cfg.ConstantTotalRPS)
	spike := buildLayer(
		uint32(len(baseline)),
		cfg.SpikePreWindow,
		cfg.SpikeBurstWindow,
		extraHFT, extraRetail, extraInst,
	)

	out := make([]topics.TaskSpec, 0, len(baseline)+len(spike))
	out = append(out, baseline...)
	out = append(out, spike...)
	return out
}

// buildRampTasks performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func buildRampTasks(cfg Config) []topics.TaskSpec {
	perWaveRPS := cfg.RampPeakRPS / rampWaveCount
	out := make([]topics.TaskSpec, 0)

	for wave := 0; wave < rampWaveCount; wave++ {
		startOffset := time.Duration(wave) * rampWaveCadence
		duration := cfg.RampDuration - startOffset
		if duration <= 0 {
			continue
		}
		hft, retail, inst, _ := botCountsForBudget(perWaveRPS)
		layer := buildLayer(uint32(len(out)), startOffset, duration, hft, retail, inst)
		out = append(out, layer...)
	}
	return out
}

// buildLayer performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func buildLayer(startTaskID uint32, startOffset, duration time.Duration,
	hftBots, retailBots, institutionalBots uint32) []topics.TaskSpec {
	total := hftBots + retailBots + institutionalBots
	tasks := make([]topics.TaskSpec, 0, total)
	id := startTaskID

	for i := uint32(0); i < hftBots; i++ {
		tasks = append(tasks, topics.TaskSpec{
			TaskID:        id,
			Profile:       "hft",
			TargetRPS:     rpsPerHFT,
			StartOffsetNs: uint64(startOffset.Nanoseconds()),
			DurationNs:    uint64(duration.Nanoseconds()),
			MarketPct:     hftMarketPct,
			CancelPct:     hftCancelPct,
			ReplacePct:    hftReplacePct,
		})
		id++
	}
	for i := uint32(0); i < retailBots; i++ {
		tasks = append(tasks, topics.TaskSpec{
			TaskID:        id,
			Profile:       "retail",
			TargetRPS:     rpsPerRetail,
			StartOffsetNs: uint64(startOffset.Nanoseconds()),
			DurationNs:    uint64(duration.Nanoseconds()),
			MarketPct:     retailMarketPct,
			CancelPct:     retailCancelPct,
			ReplacePct:    retailReplacePct,
		})
		id++
	}
	for i := uint32(0); i < institutionalBots; i++ {
		tasks = append(tasks, topics.TaskSpec{
			TaskID:        id,
			Profile:       "institutional",
			TargetRPS:     rpsPerInstitutional,
			StartOffsetNs: uint64(startOffset.Nanoseconds()),
			DurationNs:    uint64(duration.Nanoseconds()),
			MarketPct:     institutionalMarketPct,
			CancelPct:     institutionalCancelPct,
			ReplacePct:    institutionalReplacePct,
		})
		id++
	}

	return tasks
}

// newUUID performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func newUUID() (string, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return "", fmt.Errorf("uuid v7: %w", err)
	}
	return id.String(), nil
}

// envSeconds performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func envSeconds(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return def
	}
	return time.Duration(n) * time.Second
}

// envUint32 performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func envUint32(key string, def uint32) uint32 {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.ParseUint(v, 10, 32)
	if err != nil || n == 0 {
		return def
	}
	return uint32(n)
}
