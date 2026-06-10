// Unit tests for the validator's timeout handling: the startup sanity gate on
// VALIDATION_TIMEOUT vs SETTLE_DELAY, and the shape of the timed_out placeholder
// record. The invariant under test: a validation timeout must NEVER fabricate a
// violation — the status column carries the "no verdict" signal, not invented
// fill counts.
package main

import (
	"testing"
	"time"

	"github.com/iicpc/correctness-validator/internal/store"
)

// TestCheckTimeoutConfig pins the startup sanity check: a validation timeout
// that the settle delay alone would consume guarantees EVERY session hits the
// timeout fallback, so the service must refuse to start instead of silently
// recording placeholders forever.
func TestCheckTimeoutConfig(t *testing.T) {
	cases := []struct {
		name    string
		timeout time.Duration
		settle  time.Duration
		wantErr bool
	}{
		{"valid budget", 60 * time.Second, 10 * time.Second, false},
		{"zero settle delay", 60 * time.Second, 0, false},
		{"zero timeout", 0, 0, true},
		{"negative timeout", -time.Second, 0, true},
		{"timeout equals settle", 10 * time.Second, 10 * time.Second, true},
		{"timeout below settle", 5 * time.Second, 10 * time.Second, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := checkTimeoutConfig(c.timeout, c.settle)
			if (err != nil) != c.wantErr {
				t.Errorf("checkTimeoutConfig(%v, %v) = %v, wantErr=%v", c.timeout, c.settle, err, c.wantErr)
			}
		})
	}
}

// TestTimeoutRecordShape locks the timed_out placeholder's shape. The OLD
// fallback fabricated TotalFills=1/PhantomFills=1, permanently zero-scoring the
// contestant; the placeholder must instead be internally consistent — zero
// fills, zero violations — with status='timed_out' carrying the signal.
func TestTimeoutRecordShape(t *testing.T) {
	rec := timeoutRecord("sess-timeout", 42)

	if rec.Status != store.StatusTimedOut {
		t.Errorf("Status = %q, want %q", rec.Status, store.StatusTimedOut)
	}
	if rec.SessionID != "sess-timeout" || rec.ComputedAtNS != 42 {
		t.Errorf("identity fields wrong: %+v", rec)
	}
	if rec.ContestantID != "" {
		t.Errorf("ContestantID = %q, want empty (the drain never completed, so it is unknown)", rec.ContestantID)
	}
	if rec.Report.TotalFills != 0 || rec.Report.PhantomFills != 0 {
		t.Errorf("fabricated fills in timeout placeholder: %+v", rec.Report)
	}
	if rec.Report.ViolationCount() != 0 || len(rec.Report.Violations) != 0 {
		t.Errorf("timeout placeholder carries violations: %+v", rec.Report)
	}
	// 0/0 scores 1.0 by Report semantics ("nothing to get wrong"), and the
	// published 0/0 event contributes nothing to score-computer's
	// aggregateCorrectness (it sums valid and total across sessions) — neutral,
	// not a permanent zero.
	if got := rec.Report.CorrectnessScore(); got != 1.0 {
		t.Errorf("CorrectnessScore() = %v, want 1.0 for the 0/0 placeholder", got)
	}
}
