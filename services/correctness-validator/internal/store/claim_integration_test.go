package store

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/iicpc/correctness-validator/internal/validate"
)

// TestIntegration_SaveClaimIdempotent reproduces M26: Save must atomically CLAIM
// a session so only the first writer persists + (the caller) publishes. A second
// Save for the same session returns inserted=false, closing the SummaryStatus
// check-then-act race between concurrent workers. LoadScore round-trips the score.
//
// Env-gated: needs DATABASE_URL.
func TestIntegration_SaveClaimIdempotent(t *testing.T) {
	dsn := strings.TrimSpace(os.Getenv("DATABASE_URL"))
	if dsn == "" {
		t.Skip("set DATABASE_URL to run the Save-claim integration test")
	}
	ctx := context.Background()
	st, err := New(ctx, dsn)
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	defer st.Close()

	sid := fmt.Sprintf("claim-%d", time.Now().UnixNano())
	rec := Record{
		SessionID:    sid,
		ContestantID: "team-claim",
		Report:       validate.Report{TotalFills: 4, ValidFills: 2, PhantomFills: 1, Overfills: 1},
		SentCount:    1000,
		AckedCount:   950,
		MatchedCount: 940,
		ComputedAtNS: uint64(time.Now().UnixNano()),
	}

	ins1, err := st.Save(ctx, rec)
	if err != nil {
		t.Fatalf("first Save: %v", err)
	}
	if !ins1 {
		t.Fatal("first Save must report inserted=true (claimed)")
	}

	ins2, err := st.Save(ctx, rec)
	if err != nil {
		t.Fatalf("second Save: %v", err)
	}
	if ins2 {
		t.Fatal("M26: second Save for the same session must report inserted=false (already claimed)")
	}

	ev, ok, err := st.LoadScore(ctx, sid)
	if err != nil || !ok {
		t.Fatalf("LoadScore = (%v, ok=%v, err=%v)", ev, ok, err)
	}
	if ev.ContestantID != "team-claim" || ev.TotalFills != 4 || ev.ValidFills != 2 {
		t.Errorf("LoadScore round-trip wrong: %+v", ev)
	}
	// The completeness counters must survive the summary round-trip: a
	// re-published score (LoadScore path) has to carry the same coverage facts
	// as the original publish, or a redelivery would silently strip the
	// telemetry-completeness gate's inputs.
	if ev.SentCount != 1000 || ev.AckedCount != 950 || ev.MatchedCount != 940 {
		t.Errorf("LoadScore counts = sent %d acked %d matched %d, want 1000/950/940", ev.SentCount, ev.AckedCount, ev.MatchedCount)
	}
}

// TestIntegration_SaveScoredOverwritesTimeout pins the status-column upsert
// semantics behind the VALIDATION_TIMEOUT fallback redesign:
//   - a timed_out placeholder claims like any first write,
//   - a later REAL ('scored') Save overwrites the placeholder and reports
//     claimed=true — the caller owns publishing the real score,
//   - a 'scored' row is immutable: neither a repeat scored Save nor a timeout
//     placeholder may touch it (a fabricated timeout can never beat a real
//     validation to the permanent verdict).
//
// Env-gated: needs DATABASE_URL.
func TestIntegration_SaveScoredOverwritesTimeout(t *testing.T) {
	dsn := strings.TrimSpace(os.Getenv("DATABASE_URL"))
	if dsn == "" {
		t.Skip("set DATABASE_URL to run the Save-overwrite integration test")
	}
	ctx := context.Background()
	st, err := New(ctx, dsn)
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	defer st.Close()

	sid := fmt.Sprintf("status-%d", time.Now().UnixNano())

	// No row yet: SummaryStatus must report absent, not an error.
	if status, exists, err := st.SummaryStatus(ctx, sid); err != nil || exists || status != "" {
		t.Fatalf("SummaryStatus(missing) = (%q, %v, %v), want (\"\", false, nil)", status, exists, err)
	}

	placeholder := Record{
		SessionID:    sid,
		Status:       StatusTimedOut,
		ComputedAtNS: uint64(time.Now().UnixNano()),
	}
	claimed, err := st.Save(ctx, placeholder)
	if err != nil {
		t.Fatalf("Save placeholder: %v", err)
	}
	if !claimed {
		t.Fatal("first Save of the timed_out placeholder must report claimed=true")
	}
	if status, exists, err := st.SummaryStatus(ctx, sid); err != nil || !exists || status != StatusTimedOut {
		t.Fatalf("SummaryStatus after placeholder = (%q, %v, %v), want (%q, true, nil)", status, exists, err, StatusTimedOut)
	}

	// A repeat placeholder must not re-claim (timed_out never overwrites timed_out).
	if claimed, err := st.Save(ctx, placeholder); err != nil || claimed {
		t.Fatalf("repeat placeholder Save = (claimed=%v, err=%v), want (false, nil)", claimed, err)
	}

	// A REAL validation result overwrites the placeholder — and writes its
	// violation log on the overwrite path, since the claim succeeded.
	scored := Record{
		SessionID:    sid,
		ContestantID: "team-status",
		Status:       StatusScored,
		Report: validate.Report{
			TotalFills: 4, ValidFills: 3, Overfills: 1,
			Violations: []validate.Violation{{
				Type: validate.Overfill, OrderID: "T1", ReportedQty: 5, ReportedPrice: 100,
				Detail: "cumulative reported 15 exceeds order qty 10",
			}},
		},
		ComputedAtNS: uint64(time.Now().UnixNano()),
	}
	claimed, err = st.Save(ctx, scored)
	if err != nil {
		t.Fatalf("Save scored over placeholder: %v", err)
	}
	if !claimed {
		t.Fatal("scored Save over a timed_out placeholder must report claimed=true (caller publishes the real score)")
	}
	if status, exists, err := st.SummaryStatus(ctx, sid); err != nil || !exists || status != StatusScored {
		t.Fatalf("SummaryStatus after overwrite = (%q, %v, %v), want (%q, true, nil)", status, exists, err, StatusScored)
	}
	ev, ok, err := st.LoadScore(ctx, sid)
	if err != nil || !ok {
		t.Fatalf("LoadScore = (%v, ok=%v, err=%v)", ev, ok, err)
	}
	if ev.ContestantID != "team-status" || ev.TotalFills != 4 || ev.ValidFills != 3 {
		t.Errorf("LoadScore after overwrite = %+v, want team-status 4/3", ev)
	}
	var nviol int
	if err := st.pool.QueryRow(ctx,
		"SELECT count(*) FROM correctness_violations WHERE session_id=$1", sid).Scan(&nviol); err != nil {
		t.Fatalf("count violations: %v", err)
	}
	if nviol != 1 {
		t.Errorf("violation rows after overwrite = %d, want 1", nviol)
	}

	// 'scored' is immutable: a repeat scored Save does not re-claim, and a late
	// timeout placeholder can never clobber the real verdict.
	if claimed, err := st.Save(ctx, scored); err != nil || claimed {
		t.Fatalf("repeat scored Save = (claimed=%v, err=%v), want (false, nil)", claimed, err)
	}
	if claimed, err := st.Save(ctx, placeholder); err != nil || claimed {
		t.Fatalf("placeholder Save over scored = (claimed=%v, err=%v), want (false, nil)", claimed, err)
	}
	ev, ok, err = st.LoadScore(ctx, sid)
	if err != nil || !ok || ev.TotalFills != 4 || ev.ValidFills != 3 {
		t.Errorf("scored row mutated by late placeholder: (%+v, ok=%v, err=%v)", ev, ok, err)
	}
}
