package store

import (
	"strings"
	"testing"
)

// TestRankOrderByLeadsWithDisqualified pins the SQL shape of the ranking
// ORDER BY: disqualified ASC must stay the leading term so a disqualified
// run-group can never outrank a clean one on its retained measured peak.
func TestRankOrderByLeadsWithDisqualified(t *testing.T) {
	if !strings.HasPrefix(rankOrderBy, "disqualified ASC") {
		t.Fatalf("ranking order must lead with disqualified ASC, got %q", rankOrderBy)
	}
	wantOrder := []string{
		"disqualified ASC",
		"peak_sustained_tps DESC",
		"p99_at_peak_ns ASC",
		"spike_recovery_ns ASC",
		"total_correctness DESC",
		"run_group_id ASC",
	}
	last := -1
	for _, term := range wantOrder {
		i := strings.Index(rankOrderBy, term)
		if i <= last {
			t.Fatalf("term %q missing or out of order in %q", term, rankOrderBy)
		}
		last = i
	}
}

// TestScoresSortIndexMatchesRankOrder keeps the supporting index aligned with
// the ranking ORDER BY so the v2 (DQ-blind) index cannot silently come back.
func TestScoresSortIndexMatchesRankOrder(t *testing.T) {
	if strings.Contains(createTableSQL, "idx_scores_sort_v2 ON") {
		t.Fatal("createTableSQL still creates the DQ-blind idx_scores_sort_v2")
	}
	if !strings.Contains(createTableSQL, "disqualified ASC, peak_sustained_tps DESC") {
		t.Fatal("scores sort index does not lead with disqualified ASC")
	}
}

// TestCreateTableSQLTelemetryCompletenessSchema pins the telemetry-completeness
// schema evolution: every new column ships as an idempotent ADD COLUMN IF NOT
// EXISTS (the platform runs createTableSQL on every startup against live
// tables), the scores flag defaults to false so historical rows stay clean, and
// the coverage threshold is seeded at 0.90 on scoring_config.
func TestCreateTableSQLTelemetryCompletenessSchema(t *testing.T) {
	migrations := []string{
		"ALTER TABLE score_progress ADD COLUMN IF NOT EXISTS sent_count BIGINT",
		"ALTER TABLE score_progress ADD COLUMN IF NOT EXISTS acked_count BIGINT",
		"ALTER TABLE score_progress ADD COLUMN IF NOT EXISTS matched_count BIGINT",
		"ALTER TABLE scores ADD COLUMN IF NOT EXISTS incomplete_telemetry BOOLEAN NOT NULL DEFAULT false",
		"ALTER TABLE scoring_config ADD COLUMN IF NOT EXISTS min_coverage DOUBLE PRECISION NOT NULL DEFAULT 0.90",
	}
	for _, m := range migrations {
		if !strings.Contains(createTableSQL, m) {
			t.Errorf("createTableSQL missing idempotent migration %q", m)
		}
	}
	// The threshold migration must run BEFORE the seed INSERT references it,
	// or a fresh-from-old-schema startup fails on the unknown column.
	alterIdx := strings.Index(createTableSQL, "ADD COLUMN IF NOT EXISTS min_coverage")
	seedIdx := strings.Index(createTableSQL, "INSERT INTO scoring_config")
	if alterIdx == -1 || seedIdx == -1 || alterIdx > seedIdx {
		t.Errorf("min_coverage migration must precede the scoring_config seed (alter at %d, seed at %d)", alterIdx, seedIdx)
	}
	if !strings.Contains(createTableSQL, "min_coverage)") || !strings.Contains(createTableSQL, "0.90)") {
		t.Error("scoring_config seed does not include the 0.90 min_coverage default")
	}
}
