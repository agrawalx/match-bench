package controller

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/iicpc/bot-fleet-controller/internal/store"
	"github.com/jackc/pgx/v5/pgxpool"
)

const recoverySetupSQL = `
CREATE TABLE IF NOT EXISTS run_groups (
  run_group_id  TEXT PRIMARY KEY,
  submission_id TEXT NOT NULL,
  contestant_id TEXT NOT NULL DEFAULT '',
  status        TEXT NOT NULL DEFAULT 'requested',
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS runs (
  session_id    TEXT PRIMARY KEY,
  submission_id TEXT NOT NULL,
  contestant_id TEXT NOT NULL DEFAULT '',
  run_group_id  TEXT,
  scenario_id   TEXT,
  status        TEXT NOT NULL DEFAULT 'requested',
  message       TEXT NOT NULL DEFAULT '',
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_run_groups_one_active_per_submission
  ON run_groups (submission_id) WHERE status NOT IN ('completed', 'failed');`

// TestIntegration_RecoveryReleasesRunGroupIndex reproduces C4/H4: after a
// controller crash, recovery must release the one-active-per-submission unique
// index — which lives on run_groups, NOT runs. The old recovery only marked
// child runs failed (+ published an empty RunGroupID), leaving the parent group
// non-terminal forever, so re-triggering a benchmark for that submission was
// permanently rejected. This drives the real RecoverInFlightRuns against live
// Postgres + Kafka and asserts the index is freed.
//
// Env-gated: needs DATABASE_URL + KAFKA_BROKERS.
func TestIntegration_RecoveryReleasesRunGroupIndex(t *testing.T) {
	dsn := strings.TrimSpace(os.Getenv("DATABASE_URL"))
	brokers := strings.TrimSpace(os.Getenv("KAFKA_BROKERS"))
	if dsn == "" || brokers == "" {
		t.Skip("set DATABASE_URL + KAFKA_BROKERS to run the recovery integration test")
	}
	ctx := context.Background()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	defer pool.Close()
	if _, err := pool.Exec(ctx, recoverySetupSQL); err != nil {
		t.Fatalf("setup schema: %v", err)
	}

	now := time.Now().UTC()
	sub := fmt.Sprintf("sub-c4-%d", now.UnixNano())
	grp := fmt.Sprintf("rg-c4-%d", now.UnixNano())
	sess := grp + "-S"

	if _, err := pool.Exec(ctx,
		`INSERT INTO run_groups (run_group_id, submission_id, status) VALUES ($1,$2,'running')`, grp, sub); err != nil {
		t.Fatalf("insert group: %v", err)
	}
	if _, err := pool.Exec(ctx,
		`INSERT INTO runs (session_id, submission_id, run_group_id, scenario_id, status) VALUES ($1,$2,$3,'sc-1','running')`,
		sess, sub, grp); err != nil {
		t.Fatalf("insert run: %v", err)
	}

	st, err := store.New(ctx, dsn)
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}
	defer st.Close()

	// ListInFlightRuns must now carry run_group_id (the fix) so recovery can
	// target the parent group.
	runs, err := st.ListInFlightRuns(ctx)
	if err != nil {
		t.Fatalf("list in-flight: %v", err)
	}
	var found *store.InFlightRun
	for i := range runs {
		if runs[i].SessionID == sess {
			found = &runs[i]
		}
	}
	if found == nil {
		t.Fatal("our in-flight run not listed")
	}
	if found.RunGroupID != grp {
		t.Fatalf("ListInFlightRuns RunGroupID = %q, want %q (run_group_id select fix)", found.RunGroupID, grp)
	}

	// RunStatus (L38 precheck) works and reports unknown sessions as "".
	if s, _ := st.RunStatus(ctx, sess); s != "running" {
		t.Fatalf("RunStatus(sess) = %q, want running", s)
	}
	if s, _ := st.RunStatus(ctx, "no-such-session"); s != "" {
		t.Fatalf("RunStatus(unknown) = %q, want \"\"", s)
	}

	// Before recovery the index is occupied: a second active group for the same
	// submission must be rejected.
	if _, err := pool.Exec(ctx,
		`INSERT INTO run_groups (run_group_id, submission_id, status) VALUES ($1,$2,'requested')`, grp+"-2", sub); err == nil {
		t.Fatal("expected unique-index violation inserting a 2nd active group before recovery")
	}

	// Run the real recovery path (marks runs + run_groups failed, publishes).
	producer := NewProducer(brokers, slog.Default())
	defer producer.Close()
	if err := RecoverInFlightRuns(ctx, st, producer, slog.Default()); err != nil {
		t.Fatalf("recovery: %v", err)
	}

	// Child run is failed.
	if s, _ := st.RunStatus(ctx, sess); s != "failed" {
		t.Errorf("run status after recovery = %q, want failed", s)
	}
	// Parent group is failed — the index slot is freed.
	var gstatus string
	if err := pool.QueryRow(ctx, `SELECT status FROM run_groups WHERE run_group_id=$1`, grp).Scan(&gstatus); err != nil {
		t.Fatalf("read group status: %v", err)
	}
	if gstatus != "failed" {
		t.Fatalf("C4/H4: run_group status after recovery = %q, want failed (index never released)", gstatus)
	}
	// A fresh active group for the same submission now inserts successfully.
	if _, err := pool.Exec(ctx,
		`INSERT INTO run_groups (run_group_id, submission_id, status) VALUES ($1,$2,'requested')`, grp+"-3", sub); err != nil {
		t.Fatalf("re-trigger after recovery should succeed (index released), got: %v", err)
	}
}
