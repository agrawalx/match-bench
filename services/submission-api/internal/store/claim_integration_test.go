// Package store defines tests for claim integration test.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package store

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

// TestIntegration_ClaimSubmissionContestantIfEmpty performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
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

	claimed, err := st.ClaimSubmissionContestantIfEmpty(ctx, id, "winner")
	if err != nil {
		t.Fatalf("claim by winner: %v", err)
	}
	if !claimed {
		t.Fatal("claim by winner on unowned row: claimed = false, want true")
	}

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

	claimed, err = st.ClaimSubmissionContestantIfEmpty(ctx, id, "winner")
	if err != nil {
		t.Fatalf("re-claim by winner: %v", err)
	}
	if claimed {
		t.Fatal("re-claim by existing owner: claimed = true, want false")
	}

	claimed, err = st.ClaimSubmissionContestantIfEmpty(ctx, id, "")
	if err != nil {
		t.Fatalf("claim with empty contestant: %v", err)
	}
	if claimed {
		t.Fatal("claim with empty contestant: claimed = true, want false")
	}
}
