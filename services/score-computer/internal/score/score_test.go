package score

import (
	"encoding/json"
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
	if !res.Disqualified || res.PeakSustainedTPS != 0 || res.DisqualificationCode == "" {
		t.Fatalf("want dq with zero peak, got %#v", res)
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
