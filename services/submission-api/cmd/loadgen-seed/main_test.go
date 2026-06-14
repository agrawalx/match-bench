package main

import "testing"

// TestBuildScenarioLayersProfiles verifies the all-HFT throughput driver is
// preserved while a fixed, small count of retail + institutional tasks is layered
// on with their profile-standard rates and order-type mix, contiguously numbered.
func TestBuildScenarioLayersProfiles(t *testing.T) {
	const (
		target   = uint32(500_000)
		hft      = uint32(500)
		retail   = uint32(10)
		inst     = uint32(20)
		duration = uint64(120_000_000_000)
	)
	tasks := buildScenario(target, hft, retail, inst, duration)

	if got, want := uint32(len(tasks)), hft+retail+inst; got != want {
		t.Fatalf("task count = %d, want %d", got, want)
	}

	// TaskIDs contiguous 0..N-1, common timing fields uniform.
	for i, ts := range tasks {
		if ts.TaskID != uint32(i) {
			t.Fatalf("task %d has TaskID %d, want %d", i, ts.TaskID, i)
		}
		if ts.DurationNs != duration {
			t.Fatalf("task %d duration = %d, want %d", i, ts.DurationNs, duration)
		}
		if ts.StartOffsetNs != 0 {
			t.Fatalf("task %d start offset = %d, want 0", i, ts.StartOffsetNs)
		}
	}

	// HFT layer: pure new-limit, summing to exactly the target rps.
	var hftRPS uint32
	for i := uint32(0); i < hft; i++ {
		ts := tasks[i]
		if ts.Profile != "hft" {
			t.Fatalf("task %d profile = %q, want hft", i, ts.Profile)
		}
		if ts.MarketPct != 0 || ts.CancelPct != 0 || ts.ReplacePct != 0 {
			t.Fatalf("hft task %d not pure new-limit: m=%d c=%d r=%d", i, ts.MarketPct, ts.CancelPct, ts.ReplacePct)
		}
		hftRPS += ts.TargetRPS
	}
	if hftRPS != target {
		t.Fatalf("hft aggregate rps = %d, want %d", hftRPS, target)
	}

	// Retail layer: 5/s with retail mix.
	for i := hft; i < hft+retail; i++ {
		ts := tasks[i]
		if ts.Profile != "retail" || ts.TargetRPS != rpsPerRetail ||
			ts.MarketPct != retailMarketPct || ts.CancelPct != retailCancelPct || ts.ReplacePct != retailReplacePct {
			t.Fatalf("retail task %d mismatched: %+v", i, ts)
		}
	}

	// Institutional layer: 300/s with institutional mix.
	for i := hft + retail; i < hft+retail+inst; i++ {
		ts := tasks[i]
		if ts.Profile != "institutional" || ts.TargetRPS != rpsPerInstitutional ||
			ts.MarketPct != instMarketPct || ts.CancelPct != instCancelPct || ts.ReplacePct != instReplacePct {
			t.Fatalf("institutional task %d mismatched: %+v", i, ts)
		}
	}
}

// TestBuildScenarioZeroProfilesIsHFTOnly confirms the default (no retail/inst)
// is byte-for-byte the previous all-HFT behaviour.
func TestBuildScenarioZeroProfilesIsHFTOnly(t *testing.T) {
	tasks := buildScenario(100_000, 100, 0, 0, 1)
	if len(tasks) != 100 {
		t.Fatalf("task count = %d, want 100", len(tasks))
	}
	for i, ts := range tasks {
		if ts.Profile != "hft" || ts.TargetRPS != 1000 {
			t.Fatalf("task %d = %+v, want hft @ 1000/s", i, ts)
		}
	}
}
