package store

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

// TestIntegration_RecomputeRunGroupStatus reproduces H5: the parent run-group
// must NOT flip to a terminal status on the FIRST child failure while sibling
// sessions are still in flight — doing so drops it from the one-active-per-
// submission index and lets a re-trigger spawn a second active group. It becomes
// 'failed' only once ALL children are terminal and at least one failed.
//
// Env-gated: needs a live Postgres (DATABASE_URL).
func TestIntegration_RecomputeRunGroupStatus(t *testing.T) {
	dsn := strings.TrimSpace(os.Getenv("DATABASE_URL"))
	if dsn == "" {
		t.Skip("set DATABASE_URL to run the run-group rollup integration test")
	}
	ctx := context.Background()
	st, err := NewPostgresStore(ctx, dsn)
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	defer st.Close()

	now := time.Now().UTC()
	sub := fmt.Sprintf("sub-h5-%d", now.UnixNano())
	grp := fmt.Sprintf("rg-h5-%d", now.UnixNano())
	a := grp + "-A"
	b := grp + "-B"
	g := RunGroupMeta{RunGroupID: grp, SubmissionID: sub, ContestantID: "c", Status: "requested", CreatedAt: now, UpdatedAt: now}
	children := []RunMeta{
		{SessionID: a, SubmissionID: sub, RunGroupID: grp, ScenarioID: "sc-1", Status: "requested", CreatedAt: now, UpdatedAt: now},
		{SessionID: b, SubmissionID: sub, RunGroupID: grp, ScenarioID: "sc-2", Status: "requested", CreatedAt: now, UpdatedAt: now},
	}
	if err := st.InsertRunGroupWithChildren(ctx, g, children); err != nil {
		t.Fatalf("insert group: %v", err)
	}

	// One child fails while the sibling is still 'requested' -> group must stay
	// non-terminal ('running'), NOT 'failed'.
	if err := st.UpdateRunStatus(ctx, a, "failed", "boom"); err != nil {
		t.Fatalf("update A failed: %v", err)
	}
	if err := st.RecomputeRunGroupStatus(ctx, grp); err != nil {
		t.Fatalf("recompute (1): %v", err)
	}
	gm, err := st.GetRunGroup(ctx, grp)
	if err != nil {
		t.Fatalf("get group (1): %v", err)
	}
	if gm.Status == "failed" {
		t.Fatalf("H5: group went 'failed' on first child failure while sibling still in flight (status=%q)", gm.Status)
	}
	if gm.Status != "running" {
		t.Fatalf("group status with one failed + one requested = %q, want running", gm.Status)
	}

	// All children now terminal, one failed -> group is 'failed'.
	if err := st.UpdateRunStatus(ctx, b, "completed", ""); err != nil {
		t.Fatalf("update B completed: %v", err)
	}
	if err := st.RecomputeRunGroupStatus(ctx, grp); err != nil {
		t.Fatalf("recompute (2): %v", err)
	}
	gm, err = st.GetRunGroup(ctx, grp)
	if err != nil {
		t.Fatalf("get group (2): %v", err)
	}
	if gm.Status != "failed" {
		t.Fatalf("group status with all terminal + one failed = %q, want failed", gm.Status)
	}
}
