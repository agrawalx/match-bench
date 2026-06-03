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
// Save for the same session returns inserted=false, closing the HasSummary
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
}
