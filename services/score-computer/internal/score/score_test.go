package score

import (
	"encoding/json"
	"errors"
	"math"
	"testing"

	"github.com/iicpc/schemas/topics"
)

const wave = DefaultWaveDurationNS

func TestWaveScheduleDerivesRPSFromTaskIntervals(t *testing.T) {
	got := WaveSchedule([]topics.TaskSpec{
		{TargetRPS: 1000, StartOffsetNs: 0, DurationNs: 2 * wave},
		{TargetRPS: 2000, StartOffsetNs: wave, DurationNs: wave},
		{TargetRPS: 600, StartOffsetNs: wave / 2, DurationNs: wave},
	}, wave)
	want := []WaveOffer{
		{WaveIndex: 0, OfferedRPS: 1300},
		{WaveIndex: 1, OfferedRPS: 3300},
	}
	if len(got) != len(want) {
		t.Fatalf("len=%d want %d: %#v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("wave %d = %#v want %#v", i, got[i], want[i])
		}
	}
}

func TestComputePassingClimbStopsAtFirstFail(t *testing.T) {
	in := baseInput()
	in.Sessions[2].Metrics = []MetricRow{
		{WaveIndex: 0, P99NS: 500_000, ErrorRate: 0.001},
		{WaveIndex: 1, P99NS: 700_000, ErrorRate: 0.001},
		{WaveIndex: 2, P99NS: 1_500_000, ErrorRate: 0.001},
		{WaveIndex: 3, P99NS: 300_000, ErrorRate: 0.001},
	}
	res, err := Compute(in)
	if err != nil {
		t.Fatal(err)
	}
	if res.Disqualified {
		t.Fatalf("unexpected dq: %#v", res)
	}
	if res.PeakSustainedTPS != 20_000 || res.P99AtPeakNS != 700_000 {
		t.Fatalf("peak=(%d,%d), want (20000,700000)", res.PeakSustainedTPS, res.P99AtPeakNS)
	}
	if len(res.Waves) != 3 || res.Waves[2].Passed || res.Waves[2].Reason != "p99_latency" {
		t.Fatalf("bad wave walk: %#v", res.Waves)
	}
}

func TestComputeCorrectnessDQ(t *testing.T) {
	in := baseInput()
	in.Sessions[0].Correct.ValidFills = 900
	res, err := Compute(in)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Disqualified || res.PeakSustainedTPS != 30_000 || res.DisqualificationCode == "" {
		t.Fatalf("want dq with measured peak retained, got %#v", res)
	}
}

func TestComputeAbsentWaveFails(t *testing.T) {
	in := baseInput()
	in.Sessions[2].Metrics = []MetricRow{{WaveIndex: 0, P99NS: 500_000, ErrorRate: 0}}
	res, err := Compute(in)
	if err != nil {
		t.Fatal(err)
	}
	if res.PeakSustainedTPS != 10_000 || len(res.Waves) != 2 || res.Waves[1].Reason != "missing_metrics" {
		t.Fatalf("unexpected absent-wave result: %#v", res)
	}
}

func TestComputeDeterministicJSON(t *testing.T) {
	in := baseInput()
	a, err := Compute(in)
	if err != nil {
		t.Fatal(err)
	}
	b, err := Compute(in)
	if err != nil {
		t.Fatal(err)
	}
	ja, _ := json.Marshal(a)
	jb, _ := json.Marshal(b)
	if string(ja) != string(jb) {
		t.Fatalf("not deterministic:\n%s\n%s", ja, jb)
	}
}

func TestComputeNoRampDisqualifiedReturnsResult(t *testing.T) {
	in := baseInput()
	in.Sessions = in.Sessions[:2] // drop the ramp session
	in.Sessions[0].Correct.ValidFills = 800
	res, err := Compute(in)
	if err != nil {
		t.Fatalf("rampless DQ run-group must score, not error: %v", err)
	}
	if !res.Disqualified || res.DisqualificationCode != "correctness_below_threshold" {
		t.Fatalf("want dq result, got %#v", res)
	}
	if res.PeakSustainedTPS != 0 {
		t.Fatalf("rampless run-group should not retain a peak: %#v", res)
	}
}

func TestComputeNoRampWithoutDQReturnsError(t *testing.T) {
	in := baseInput()
	in.Sessions = in.Sessions[:2] // drop the ramp session
	if _, err := Compute(in); !errors.Is(err, ErrMissingRampSession) {
		t.Fatalf("err = %v, want ErrMissingRampSession", err)
	}
}

// TestComputeIncompleteTelemetrySkipsViolationDQ pins the telemetry-completeness
// gate: when any session's acked-matched coverage (matched_count/sent_count)
// falls below the threshold, violations are unsound evidence — a lost sent
// flush fabricates phantoms, a lost acked event silently drops orders from the
// replay — so the ramp_session_violation DQ must NOT fire. The run is flagged
// IncompleteTelemetry with the reason in score_detail instead; throughput waves
// are still scored normally.
func TestComputeIncompleteTelemetrySkipsViolationDQ(t *testing.T) {
	in := baseInput()
	// Ramp session reports a few violations but its correctness RATIO stays 1.0
	// (ValidFills==TotalFills); coverage 850/1000 = 0.85 is below the 0.90
	// threshold, so the run is flagged IncompleteTelemetry.
	in.Sessions[2].Correct.ViolationCount = 1
	in.Sessions[2].Correct.SentCount = 1000
	in.Sessions[2].Correct.AckedCount = 900
	in.Sessions[2].Correct.MatchedCount = 850
	res, err := Compute(in)
	if err != nil {
		t.Fatal(err)
	}
	// A high correctness ratio never disqualifies regardless of violation count
	// (under-reporting and order-dependent breaks are not ratio failures).
	if res.Disqualified || res.DisqualificationCode != "" {
		t.Fatalf("high-ratio ramp must not be disqualified: %#v", res)
	}
	if !res.IncompleteTelemetry {
		t.Fatalf("IncompleteTelemetry not set: %#v", res)
	}
	if res.IncompleteTelemetryReason == "" {
		t.Fatalf("reason missing from score detail: %#v", res)
	}
	if res.PeakSustainedTPS != 30_000 {
		t.Fatalf("throughput waves must still be scored: %#v", res)
	}
}

// TestComputeCoverageAtThresholdKeepsViolationDQ pins the unchanged path:
// coverage at/above the threshold means the inputs are complete enough, so a
// ramp violation disqualifies exactly as before and the flag stays false.
func TestComputeCoverageAtThresholdKeepsViolationDQ(t *testing.T) {
	in := baseInput()
	// Ramp correctness ratio 850/1000 = 0.85 < 0.95 (aggregate stays at 0.95,
	// so the per-session gate is what fires); coverage 950/1000 = 0.95 >= 0.90,
	// so the run is NOT flagged incomplete.
	in.Sessions[2].Correct.ValidFills = 850
	in.Sessions[2].Correct.SentCount = 1000
	in.Sessions[2].Correct.AckedCount = 960
	in.Sessions[2].Correct.MatchedCount = 950 // 0.95 >= 0.90
	res, err := Compute(in)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Disqualified || res.DisqualificationCode != "session_correctness_below_threshold" {
		t.Fatalf("a ramp below the correctness threshold must be disqualified: %#v", res)
	}
	if res.IncompleteTelemetry || res.IncompleteTelemetryReason != "" {
		t.Fatalf("flag must stay false at/above the coverage threshold: %#v", res)
	}
}

// TestComputeIncompleteTelemetryKeepsCorrectnessGates pins the asymmetry:
// the correctness-RATIO gates stay active on incomplete telemetry (a ratio is
// less sensitive to uniform loss than absolute violation counts), so a
// below-threshold correctness still disqualifies even when the run is flagged.
func TestComputeIncompleteTelemetryKeepsCorrectnessGates(t *testing.T) {
	in := baseInput()
	in.Sessions[0].Correct.ValidFills = 800 // 0.933 aggregate < 0.95 DQ threshold
	in.Sessions[2].Correct.SentCount = 1000
	in.Sessions[2].Correct.MatchedCount = 100 // 0.10 coverage — grossly incomplete
	res, err := Compute(in)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Disqualified || res.DisqualificationCode != "correctness_below_threshold" {
		t.Fatalf("correctness-ratio gate must survive the completeness flag: %#v", res)
	}
	if !res.IncompleteTelemetry {
		t.Fatalf("IncompleteTelemetry not set: %#v", res)
	}
}

// TestComputeUnknownCoverageNotGated pins the legacy/rollout behavior: a
// session with sent_count==0 (a row written before the counters existed, or a
// timed_out placeholder whose drain never completed) has UNKNOWN coverage —
// the gate must not fire, and pre-gate behavior (including violation DQ) is
// preserved rather than retroactively reflagging historical runs.
func TestComputeUnknownCoverageNotGated(t *testing.T) {
	in := baseInput()
	// sent_count==0 everywhere => coverage unknown => not flagged. The ramp's
	// correctness ratio 850/1000 = 0.85 < 0.95 still disqualifies via the
	// per-session gate (ratio gates do not depend on coverage).
	in.Sessions[2].Correct.ValidFills = 850
	res, err := Compute(in)
	if err != nil {
		t.Fatal(err)
	}
	if res.IncompleteTelemetry || res.IncompleteTelemetryReason != "" {
		t.Fatalf("unknown coverage must not flag the run: %#v", res)
	}
	if !res.Disqualified || res.DisqualificationCode != "session_correctness_below_threshold" {
		t.Fatalf("ramp below correctness threshold must disqualify: %#v", res)
	}
}

// TestConfigWithDefaultsMinCoverage pins the threshold plumbing: zero/invalid
// values fall back to the 0.90 default, valid stored values are kept.
func TestConfigWithDefaultsMinCoverage(t *testing.T) {
	cases := []struct {
		name string
		in   float64
		want float64
	}{
		{"zero falls back", 0, DefaultMinCoverage},
		{"negative falls back", -1, DefaultMinCoverage},
		{"above one falls back", 1.5, DefaultMinCoverage},
		{"NaN falls back", math.NaN(), DefaultMinCoverage},
		{"valid kept", 0.8, 0.8},
		{"one kept", 1, 1},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cfg := Config{MinCoverage: c.in}.WithDefaults()
			if cfg.MinCoverage != c.want {
				t.Errorf("MinCoverage = %v, want %v", cfg.MinCoverage, c.want)
			}
		})
	}
}

func TestSortResultsDisqualifiedRanksLast(t *testing.T) {
	results := []Result{
		{RunGroupID: "dq", PeakSustainedTPS: 1_000_000, P99AtPeakNS: 1, TotalCorrectness: 1, Disqualified: true},
		{RunGroupID: "slow", PeakSustainedTPS: 10, P99AtPeakNS: 900_000, TotalCorrectness: 0.99},
		{RunGroupID: "fast", PeakSustainedTPS: 50_000, P99AtPeakNS: 400_000, TotalCorrectness: 1},
	}
	SortResults(results)
	if results[len(results)-1].RunGroupID != "dq" {
		t.Fatalf("dq with retained peak must rank below every clean result: %#v", results)
	}
	if results[0].RunGroupID != "fast" || results[1].RunGroupID != "slow" {
		t.Fatalf("clean results lost their relative order: %#v", results)
	}
}

func TestSortResultsTiebreak(t *testing.T) {
	results := []Result{
		{RunGroupID: "b", PeakSustainedTPS: 20, P99AtPeakNS: 10, SpikeRecoveryNS: 5, TotalCorrectness: 0.999},
		{RunGroupID: "a", PeakSustainedTPS: 20, P99AtPeakNS: 5, SpikeRecoveryNS: 100, TotalCorrectness: 1},
		{RunGroupID: "c", PeakSustainedTPS: 30, P99AtPeakNS: 50, SpikeRecoveryNS: 1, TotalCorrectness: 0.99},
	}
	SortResults(results)
	if results[0].RunGroupID != "c" || results[1].RunGroupID != "a" || results[2].RunGroupID != "b" {
		t.Fatalf("bad order: %#v", results)
	}
}

func TestAggregateCorrectnessDoesNotOverflow(t *testing.T) {
	got := aggregateCorrectness([]Session{
		{Correct: Correctness{ValidFills: math.MaxUint64, TotalFills: math.MaxUint64}},
		{Correct: Correctness{ValidFills: math.MaxUint64, TotalFills: math.MaxUint64}},
	})
	if got != 1 {
		t.Fatalf("correctness = %v, want 1", got)
	}
}

func TestWaveScheduleBoundsOverflowedTaskEnd(t *testing.T) {
	got := WaveSchedule([]topics.TaskSpec{
		{TargetRPS: 1, StartOffsetNs: math.MaxUint64 - 1, DurationNs: math.MaxUint64},
	}, 1)
	if len(got) != 0 {
		t.Fatalf("overflowed late task should not allocate or wrap into early waves: %#v", got[:min(len(got), 3)])
	}
}

func baseInput() Input {
	correct := Correctness{ValidFills: 1000, TotalFills: 1000}
	return Input{
		RunGroupID:   "rg",
		SubmissionID: "sub",
		ContestantID: "contestant",
		Sessions: []Session{
			{SessionID: "constant", Scenario: "constant", Correct: correct},
			{SessionID: "spike", Scenario: "spike", Correct: correct},
			{
				SessionID: "ramp",
				Scenario:  "ramp",
				Correct:   correct,
				TaskSpecs: []topics.TaskSpec{
					{TargetRPS: 10_000, StartOffsetNs: 0, DurationNs: wave},
					{TargetRPS: 20_000, StartOffsetNs: wave, DurationNs: wave},
					{TargetRPS: 30_000, StartOffsetNs: 2 * wave, DurationNs: wave},
				},
				Metrics: []MetricRow{
					{WaveIndex: 0, P99NS: 500_000, ErrorRate: 0},
					{WaveIndex: 1, P99NS: 700_000, ErrorRate: 0},
					{WaveIndex: 2, P99NS: 900_000, ErrorRate: 0},
				},
			},
		},
	}
}
