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
//     Institutional 300. Tunable later by editing the scenarios table rows
//     directly — judges do not need a code change to retune.
//   - Total RPS baseline = 10,000. Same baseline mix is reused inside spike
//     and ramp.
//   - No jitter, no burstiness. Strictly paced per task.
//
// The builder is pure: no I/O, no clock reads, deterministic given a UUID
// generator. Tests in builder_test.go assert the totals and shape.
package scenarios

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/iicpc/schemas/topics"
)

// Profile RPS values (midpoints of the arch_v2 ranges). Tunable via the
// scenarios table after seed; these values are only the v1 defaults.
const (
	rpsPerHFT           uint32 = 1000
	rpsPerRetail        uint32 = 5
	rpsPerInstitutional uint32 = 300
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

// Baseline RPS budgets per profile, derived from the 60/25/15 contribution
// mix at a total of 10,000 RPS. Held as constants so the bot-counts below
// fall out by simple integer division.
const (
	baselineTotalRPS            uint32 = 10000
	baselineHFTRPS              uint32 = 6000  // 60% of 10k
	baselineRetailRPS           uint32 = 2500  // 25% of 10k
	baselineInstitutionalRPS    uint32 = 1500  // 15% of 10k
)

// Baseline bot counts — RPS budget ÷ per-bot RPS.
//   HFT:          6000 / 1000 = 6
//   Retail:       2500 /    5 = 500
//   Institutional: 1500 /  300 = 5
const (
	baselineHFTBots           uint32 = 6
	baselineRetailBots        uint32 = 500
	baselineInstitutionalBots uint32 = 5
)

// Session durations and shape parameters.
const (
	constantDuration       = 60 * time.Second
	spikeDuration          = 60 * time.Second
	spikePreWindow         = 25 * time.Second
	spikeBurstWindow       = 10 * time.Second
	spikeMultiplierOverBaseline = 4 // baseline + 4x baseline = 5x total during spike

	rampDuration       = 180 * time.Second
	rampWaveCount      = 9              // 9 waves on a 20s cadence; peak ≈ 90k
	rampWaveCadence    = 20 * time.Second
)

// BuildAll returns the three canonical scenarios. Each ScenarioRow carries
// a freshly minted scenario_id; submission-api passes the result straight
// to SeedScenarios which inserts ON CONFLICT (name) DO NOTHING, so re-running
// is idempotent and does not change scenario_ids already in the database.
func BuildAll() ([]ScenarioRow, error) {
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
			DurationNs: uint64(constantDuration.Nanoseconds()),
			TaskSpecs:  buildConstantTasks(0, constantDuration),
		},
		{
			ScenarioID: spikeID,
			Name:       "spike",
			SortOrder:  2,
			DurationNs: uint64(spikeDuration.Nanoseconds()),
			TaskSpecs:  buildSpikeTasks(),
		},
		{
			ScenarioID: rampID,
			Name:       "ramp",
			SortOrder:  3,
			DurationNs: uint64(rampDuration.Nanoseconds()),
			TaskSpecs:  buildRampTasks(),
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

// buildConstantTasks emits the baseline mix as task entries with the given
// start offset and duration. Used both for the standalone Constant scenario
// and as the baseline layer inside Spike and Ramp.
//
// Task IDs start at startTaskID + 0 and ascend; callers compose multiple
// layers by passing a non-zero startTaskID so the IDs across layers stay
// unique within one scenario.
func buildConstantTasks(startTaskID uint32, duration time.Duration) []topics.TaskSpec {
	return buildLayer(startTaskID, 0, duration,
		baselineHFTBots, baselineRetailBots, baselineInstitutionalBots)
}

// buildSpikeTasks emits a baseline layer for the full duration plus a spike
// layer that fires only during the burst window. The spike layer carries
// `spikeMultiplierOverBaseline x` additional bots of each profile, so total
// active RPS during the burst is 5x the baseline.
func buildSpikeTasks() []topics.TaskSpec {
	baseline := buildConstantTasks(0, spikeDuration)

	// Spike-extra layer: same per-bot rates, 4x the baseline bot counts, fires
	// only during the 10s burst window.
	spike := buildLayer(
		uint32(len(baseline)),
		spikePreWindow,
		spikeBurstWindow,
		baselineHFTBots*spikeMultiplierOverBaseline,
		baselineRetailBots*spikeMultiplierOverBaseline,
		baselineInstitutionalBots*spikeMultiplierOverBaseline,
	)

	out := make([]topics.TaskSpec, 0, len(baseline)+len(spike))
	out = append(out, baseline...)
	out = append(out, spike...)
	return out
}

// buildRampTasks emits 9 stacked waves on a 20s cadence. Wave N has start
// offset N*20s and duration (rampDuration - N*20s) so every wave terminates
// at the end of the ramp window — the staircase only ever ramps up. Peak
// active rate ≈ 9 x 10k = 90k RPS, held from t=160s to t=180s.
//
// Note vs. the architecture brief: the original "100k peak" target rounded
// to "9 waves at 20s cadence" gives 90k, which is the cleanest integer fit.
// If a 100k peak is wanted exactly, change rampWaveCount to 10 and reduce
// the cadence to 18s (or accept the last wave starting at t=180 with zero
// duration, which the current ramp formula already rejects).
func buildRampTasks() []topics.TaskSpec {
	tasksPerWave := baselineHFTBots + baselineRetailBots + baselineInstitutionalBots
	out := make([]topics.TaskSpec, 0, int(tasksPerWave)*rampWaveCount)

	for wave := 0; wave < rampWaveCount; wave++ {
		startOffset := time.Duration(wave) * rampWaveCadence
		duration := rampDuration - startOffset
		if duration <= 0 {
			continue
		}
		layer := buildLayer(
			uint32(len(out)),
			startOffset,
			duration,
			baselineHFTBots, baselineRetailBots, baselineInstitutionalBots,
		)
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
