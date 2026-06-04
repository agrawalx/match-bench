package scenarios

import (
	"testing"
	"time"

	"github.com/iicpc/schemas/topics"
)

// sumRPSAt returns the total active RPS at time t (relative to barrier epoch)
// across the given task list. Used to verify that scenarios hit their
// documented RPS targets at the documented phases.
func sumRPSAt(specs []topics.TaskSpec, t time.Duration) uint64 {
	var total uint64
	for _, s := range specs {
		start := time.Duration(s.StartOffsetNs)
		end := start + time.Duration(s.DurationNs)
		if t >= start && t < end {
			total += uint64(s.TargetRPS)
		}
	}
	return total
}

func TestConstantScenario_FlatBaseline(t *testing.T) {
	rows, err := BuildAll(DefaultConfig())
	if err != nil {
		t.Fatalf("BuildAll: %v", err)
	}
	c := find(t, rows, "constant")

	want := uint64(DefaultConfig().ConstantTotalRPS)
	for _, at := range []time.Duration{0, 30 * time.Second, 59 * time.Second} {
		got := sumRPSAt(c.TaskSpecs, at)
		if got != want {
			t.Errorf("constant: RPS at t=%v = %d, want %d", at, got, want)
		}
	}
}

func TestSpikeScenario_Shape(t *testing.T) {
	rows, err := BuildAll(DefaultConfig())
	if err != nil {
		t.Fatalf("BuildAll: %v", err)
	}
	s := find(t, rows, "spike")

	cases := []struct {
		at   time.Duration
		want uint64
	}{
		{at: 0, want: 10_000},                // pre-spike baseline
		{at: 24 * time.Second, want: 10_000}, // still pre-spike
		{at: 25 * time.Second, want: 50_000}, // spike window opens
		{at: 30 * time.Second, want: 50_000}, // mid-spike
		{at: 34 * time.Second, want: 50_000}, // last full second of spike
		{at: 35 * time.Second, want: 10_000}, // back to baseline
		{at: 59 * time.Second, want: 10_000}, // tail baseline
	}
	for _, c := range cases {
		got := sumRPSAt(s.TaskSpecs, c.at)
		if got != c.want {
			t.Errorf("spike: RPS at t=%v = %d, want %d", c.at, got, c.want)
		}
	}
}

func TestRampScenario_Staircase(t *testing.T) {
	rows, err := BuildAll(DefaultConfig())
	if err != nil {
		t.Fatalf("BuildAll: %v", err)
	}
	r := find(t, rows, "ramp")

	perWave := uint64(DefaultConfig().RampPeakRPS / rampWaveCount)
	// Probe one second past each wave boundary so assertions are not
	// sensitive to whether a wave starts at exactly the boundary or one
	// nanosecond later.
	for wave := 0; wave < rampWaveCount; wave++ {
		at := time.Duration(wave)*rampWaveCadence + 1*time.Second
		expected := perWave * uint64(wave+1)
		got := sumRPSAt(r.TaskSpecs, at)
		if got != expected {
			t.Errorf("ramp: RPS at t=%v (after wave %d) = %d, want %d",
				at, wave, got, expected)
		}
	}

	// Peak holds until the end of the ramp duration.
	peak := perWave * uint64(rampWaveCount)
	got := sumRPSAt(r.TaskSpecs, 179*time.Second)
	if got != peak {
		t.Errorf("ramp: RPS at t=179s = %d, want peak %d", got, peak)
	}
}

// TestConfig_CustomDurationAndRPS verifies the operator knobs flow through:
// a 300s constant at 50k RPS produces a 300s scenario whose tasks all run the
// full window and sum to 50k, with the 60/25/15 mix preserved.
func TestConfig_CustomDurationAndRPS(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ConstantDuration = 300 * time.Second
	cfg.ConstantTotalRPS = 50000
	cfg.SpikePeakRPS = 200000

	rows, err := BuildAll(cfg)
	if err != nil {
		t.Fatalf("BuildAll: %v", err)
	}
	c := find(t, rows, "constant")

	if c.DurationNs != uint64((300 * time.Second).Nanoseconds()) {
		t.Errorf("constant duration = %d ns, want 300s", c.DurationNs)
	}
	// At t=0 and near the end the full baseline must be active.
	for _, at := range []time.Duration{0, 150 * time.Second, 299 * time.Second} {
		if got := sumRPSAt(c.TaskSpecs, at); got != 50000 {
			t.Errorf("constant@50k: RPS at t=%v = %d, want 50000", at, got)
		}
	}

	// Spike peak knob: burst window reaches 200k, baseline stays 50k.
	s := find(t, rows, "spike")
	pre := cfg.SpikePreWindow
	if got := sumRPSAt(s.TaskSpecs, pre-time.Second); got != 50000 {
		t.Errorf("spike pre-burst RPS = %d, want 50000", got)
	}
	if got := sumRPSAt(s.TaskSpecs, pre+time.Second); got != 200000 {
		t.Errorf("spike burst RPS = %d, want 200000", got)
	}
}

// TestConfig_Validation rejects nonsense configs.
func TestConfig_Validation(t *testing.T) {
	bad := []Config{
		func() Config { c := DefaultConfig(); c.ConstantDuration = 0; return c }(),
		func() Config { c := DefaultConfig(); c.ConstantTotalRPS = 0; return c }(),
		func() Config { c := DefaultConfig(); c.SpikePeakRPS = 1000; return c }(), // < baseline
	}
	for i, c := range bad {
		if _, err := BuildAll(c); err == nil {
			t.Errorf("case %d: expected validation error, got nil", i)
		}
	}
}

func find(t *testing.T, rows []ScenarioRow, name string) ScenarioRow {
	t.Helper()
	for _, r := range rows {
		if r.Name == name {
			return r
		}
	}
	t.Fatalf("scenario %q not found", name)
	return ScenarioRow{}
}
