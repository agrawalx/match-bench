package store

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

// TestIntegration_ClaimSubmissionContestantIfEmpty pins the claim-race
// contract: the UPDATE ... WHERE contestant_id = ” is the atomic
// first-writer-wins gate, and callers can only distinguish "I won" from
// "someone else owns this" via the claimed return derived from RowsAffected.
// The pre-fix signature returned only error, so racing callers both assumed
// ownership and the loser operated on another contestant's submission.
//
// Env-gated: needs a live Postgres (DATABASE_URL).
func TestIntegration_ClaimSubmissionContestantIfEmpty(t *testing.T) {
	dsn := strings.TrimSpace(os.Getenv("DATABASE_URL"))
	if dsn == "" {
		t.Skip("set DATABASE_URL to run the claim integration test")
	}
	ctx := context.Background()
	st, err := NewPostgresStore(ctx, dsn)
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	defer st.Close()

	id := fmt.Sprintf("sub-claim-%d", time.Now().UnixNano())
	if err := st.Insert(ctx, SubmissionMeta{
		SubmissionID: id, ContestantID: "", SHA256: id, Language: "go",
		Protocol: "FIX", Port: 9898, TeamName: "t", ArtifactPath: "/x",
		Status: "ready", CreatedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("insert: %v", err)
	}

	// First claim on an unowned row wins.
	claimed, err := st.ClaimSubmissionContestantIfEmpty(ctx, id, "winner")
	if err != nil {
		t.Fatalf("claim by winner: %v", err)
	}
	if !claimed {
		t.Fatal("claim by winner on unowned row: claimed = false, want true")
	}

	// A second contestant loses the race — claimed must be false, and the
	// DB row must still belong to the winner.
	claimed, err = st.ClaimSubmissionContestantIfEmpty(ctx, id, "loser")
	if err != nil {
		t.Fatalf("claim by loser: %v", err)
	}
	if claimed {
		t.Fatal("claim by loser on owned row: claimed = true, want false")
	}
	meta, err := st.GetByID(ctx, id)
	if err != nil {
		t.Fatalf("get after losing claim: %v", err)
	}
	if meta.ContestantID != "winner" {
		t.Fatalf("contestant_id after losing claim = %q, want winner", meta.ContestantID)
	}

	// Re-claim by the existing owner is also not a fresh claim (the guard is
	// contestant_id = '', not idempotent-per-owner) — callers must re-read
	// and compare against the DB value, which the handler helper does.
	claimed, err = st.ClaimSubmissionContestantIfEmpty(ctx, id, "winner")
	if err != nil {
		t.Fatalf("re-claim by winner: %v", err)
	}
	if claimed {
		t.Fatal("re-claim by existing owner: claimed = true, want false")
	}

	// Empty contestant ID is a no-op, never a claim.
	claimed, err = st.ClaimSubmissionContestantIfEmpty(ctx, id, "")
	if err != nil {
		t.Fatalf("claim with empty contestant: %v", err)
	}
	if claimed {
		t.Fatal("claim with empty contestant: claimed = true, want false")
	}
}
