// Package scenarios builds the three canonical load-test scenarios that
// every contestant runs as part of a benchmark trigger: constant, spike, and
// ramp. The builder is the single source of truth for the participant mix
// and per-bot RPS values; submission-api seeds the scenarios table from the
// builder's output on startup.
//
// Decisions encoded here (locked in architecture_v2.md §"Load Scenarios" and
// the locked-decisions block at the bottom of that section):
//
//   - Participant mix by RPS contribution, not bot count:
//     HFT 60%, Retail 25%, Institutional 15%.
//   - Per-bot RPS midpoints of the arch_v2 ranges: HFT 1000, Retail 5,
//     Institutional 300. These are the per-bot granularity; the total RPS of a
//     scenario is a configurable budget (see Config) and the bot COUNTS fall
//     out of that budget divided by the per-bot rates.
//   - No jitter, no burstiness. Strictly paced per task.
//
// Configurability: the durations and total/peak RPS budgets are operator knobs
// (Config, populated from env by ConfigFromEnv). Changing them and restarting
// submission-api with RESEED_SCENARIOS=true rewrites the scenario rows. The
// participant mix (60/25/15) and per-bot rates stay fixed so the realistic
// order-type distribution is preserved at every load level; the mix self-evens
// across pods as the budget scales (a larger budget yields more, finely
// divisible bots).
//
// The builder is pure: no I/O, no clock reads, deterministic given a UUID
// generator. Tests in builder_test.go assert the totals and shape.
package scenarios

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/iicpc/schemas/topics"
)

// Profile RPS values (midpoints of the arch_v2 ranges). These are the per-bot
// granularity at which a total-RPS budget is realised; they are intentionally
// NOT operator knobs — the total budget is (see Config).
const (
	rpsPerHFT           uint32 = 1000
	rpsPerRetail        uint32 = 5
	rpsPerInstitutional uint32 = 300
)

// Participant mix by RPS contribution. A scenario's total RPS budget is split
// across the three profiles by these percentages, then each profile's slice is
// divided by its per-bot rate to get an integer bot count.
const (
	mixHFTPct           uint32 = 60
	mixRetailPct        uint32 = 25
	mixInstitutionalPct uint32 = 15
)

// Per-profile order-type mix, as a percentage of messages sent. Source:
// architecture_v2.md Bot Profiles. The limit fraction is implied
// (100 - market - cancel - replace). Tunable via the scenarios table after seed.
//
// arch_v2 lists HFT as 50% limit / 10% market / 40% cancel. We split that 40%
// "cancel" into 30% cancel + 10% cancel/replace so the cancel-replace
// priority-loss validator path gets coverage; the 50/10/40 limit/market/mutation
// split is preserved. Retail (30/65/5) and Institutional (80/20/0) match arch_v2
// exactly and do not issue replaces.
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

// Ramp shape parameters. The ramp stacks rampWaveCount baseline layers on a
// rampWaveCadence schedule; the per-wave budget is RampPeakRPS / rampWaveCount
// so the top wave reaches RampPeakRPS. These are fixed shape constants; the
// peak magnitude and total duration are operator knobs (Config).
const (
	rampWaveCount   = 9 // 9 waves; peak ≈ RampPeakRPS held at the top of the ramp
	rampWaveCadence = 20 * time.Second
)

// Config holds the operator-tunable load parameters. ConfigFromEnv populates it
// from the environment; DefaultConfig() returns the v1 locked defaults so an
// unconfigured deployment behaves exactly as before.
type Config struct {
	ConstantDuration time.Duration
	SpikeDuration    time.Duration
	RampDuration     time.Duration

	// ConstantTotalRPS is the aggregate orders/sec for the constant scenario
	// and the baseline layer of the spike scenario.
	ConstantTotalRPS uint32
	// SpikePeakRPS is the aggregate orders/sec the spike scenario reaches
	// during its burst window. Must be >= ConstantTotalRPS.
	SpikePeakRPS uint32
	// RampPeakRPS is the aggregate orders/sec at the top of the ramp staircase.
	RampPeakRPS uint32

	// SpikePreWindow is how long the spike holds at baseline before the burst;
	// SpikeBurstWindow is how long the burst lasts. The burst occupies
	// [SpikePreWindow, SpikePreWindow+SpikeBurstWindow).
	SpikePreWindow   time.Duration
	SpikeBurstWindow time.Duration
}

// DefaultConfig returns the v1 locked defaults (architecture_v2.md §"Load
// Scenarios"): constant/spike 60s, ramp 180s; baseline 10k RPS, spike peak 50k
// (5×), ramp peak 90k (9×10k).
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

// ConfigFromEnv returns DefaultConfig overridden by any of these env vars:
//
//	CONSTANT_DURATION_S, SPIKE_DURATION_S, RAMP_DURATION_S   (seconds)
//	CONSTANT_TOTAL_RPS, SPIKE_PEAK_RPS, RAMP_PEAK_RPS        (orders/sec)
//	SPIKE_PREWINDOW_S, SPIKE_BURST_S                         (seconds)
//
// An unset or malformed value leaves the default in place.
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

// BuildAll returns the three canonical scenarios sized by cfg. Each ScenarioRow
// carries a freshly minted scenario_id; submission-api passes the result to
// SeedScenarios. On a re-seed (RESEED_SCENARIOS=true) the store preserves the
// existing row's scenario_id and overwrites only duration/task_specs, so a UUID
// minted here is used only for a first insert.
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

	// SortOrder defines the execution sequence within a run-group:
	// 1=constant (baseline), 2=spike (stress burst), 3=ramp (knee finder).
	// ListScenarios + ListRunsByGroup order by this column so the frontend
	// renders the three sessions in execution order, not alphabetical.
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

// ScenarioRow mirrors the store.ScenarioRow shape so the builder package
// does not need to import the store package (avoiding an import cycle when
// scenarios is referenced from anywhere else). main.go casts between them.
type ScenarioRow struct {
	ScenarioID string
	Name       string
	SortOrder  int
	DurationNs uint64
	TaskSpecs  []topics.TaskSpec
}

// botCountsForBudget splits a total RPS budget across the three profiles by the
// fixed 60/25/15 contribution mix, then divides each profile's slice by its
// per-bot rate to get an integer bot count. Integer division may drop a few RPS
// at the margin when the budget is not a clean multiple of the per-bot rates;
// realisedRPS reports the actual aggregate the returned counts produce.
func botCountsForBudget(totalRPS uint32) (hft, retail, inst, realisedRPS uint32) {
	hft = (totalRPS * mixHFTPct / 100) / rpsPerHFT
	retail = (totalRPS * mixRetailPct / 100) / rpsPerRetail
	inst = (totalRPS * mixInstitutionalPct / 100) / rpsPerInstitutional
	realisedRPS = hft*rpsPerHFT + retail*rpsPerRetail + inst*rpsPerInstitutional
	return hft, retail, inst, realisedRPS
}

// buildConstantTasks emits the participant mix sized to totalRPS as task
// entries with the given start offset and duration. Used both for the
// standalone Constant scenario and as the baseline layer inside Spike and Ramp.
func buildConstantTasks(startTaskID uint32, startOffset, duration time.Duration, totalRPS uint32) []topics.TaskSpec {
	hft, retail, inst, _ := botCountsForBudget(totalRPS)
	return buildLayer(startTaskID, startOffset, duration, hft, retail, inst)
}

// buildSpikeTasks emits a baseline layer (ConstantTotalRPS) for the full
// duration plus a spike layer sized to (SpikePeakRPS - ConstantTotalRPS) that
// fires only during the burst window, so total active RPS during the burst is
// SpikePeakRPS.
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

// buildRampTasks emits rampWaveCount stacked waves on a rampWaveCadence
// schedule. Each wave carries a per-wave budget of RampPeakRPS / rampWaveCount
// and starts at offset wave*cadence, lasting until the end of the ramp window —
// the staircase only ever ramps up. Peak active rate ≈ RampPeakRPS, held from
// the last wave's start to the end of the window.
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

// buildLayer emits the participant mix as task entries with a common start
// offset and duration. Profile RPS values are taken from the package
// constants. Task IDs run from startTaskID upwards.
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

func newUUID() (string, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return "", fmt.Errorf("uuid v7: %w", err)
	}
	return id.String(), nil
}

// envSeconds reads an integer-seconds env var into a Duration, falling back to
// def when unset or malformed.
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

// envUint32 reads a uint32 env var, falling back to def when unset or malformed.
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
