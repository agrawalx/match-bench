// Package read defines tests for store test.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package read

import (
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

// TestLeaderboardOrderByAllowlist performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestLeaderboardOrderByAllowlist(t *testing.T) {
	got := leaderboardOrderBy("p99", "desc")
	if !strings.HasPrefix(got, "disqualified ASC, p99_at_peak_ns DESC") {
		t.Fatalf("order = %q", got)
	}

	injected := leaderboardOrderBy("p99;DROP TABLE scores", "desc;DROP")
	if strings.Contains(injected, ";") || !strings.HasPrefix(injected, "disqualified ASC, rank ASC") {
		t.Fatalf("unsafe fallback order = %q", injected)
	}
}

// TestLeaderboardOrderByDisqualifiedAlwaysLeads performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestLeaderboardOrderByDisqualifiedAlwaysLeads(t *testing.T) {
	cases := []struct {
		name string
		sort string
		ord  string
	}{
		{name: "default", sort: "", ord: ""},
		{name: "rank", sort: "rank", ord: "asc"},
		{name: "peak_tps_desc", sort: "peak_tps", ord: "desc"},
		{name: "peak_sustained_tps_asc", sort: "peak_sustained_tps", ord: "asc"},
		{name: "p99", sort: "p99", ord: ""},
		{name: "p99_at_peak", sort: "p99_at_peak", ord: "desc"},
		{name: "p99_ns_at_peak_tps", sort: "p99_ns_at_peak_tps", ord: "asc"},
		{name: "spike_recovery", sort: "spike_recovery", ord: ""},
		{name: "spike_recovery_ns", sort: "spike_recovery_ns", ord: "desc"},
		{name: "total_score", sort: "total_score", ord: ""},
		{name: "total_correctness", sort: "total_correctness", ord: "asc"},
		{name: "correctness", sort: "correctness", ord: "desc"},
		{name: "team_name", sort: "team_name", ord: ""},
		{name: "computed_at", sort: "computed_at", ord: "desc"},
		{name: "hyphen_alias", sort: "peak-tps", ord: "desc"},
		{name: "unknown_falls_back", sort: "disqualified", ord: "asc"},
		{name: "injection_falls_back", sort: "peak_tps;--", ord: "desc"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := leaderboardOrderBy(tc.sort, tc.ord)
			if !strings.HasPrefix(got, "disqualified ASC, ") {
				t.Fatalf("sort=%q order=%q produced %q without leading disqualified ASC", tc.sort, tc.ord, got)
			}
		})
	}
}

// TestRankedSubqueryOrderLeadsWithDisqualified performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestRankedSubqueryOrderLeadsWithDisqualified(t *testing.T) {
	if !strings.HasPrefix(rankedOrder, "disqualified ASC") {
		t.Fatalf("ranked subquery order must lead with disqualified ASC, got %q", rankedOrder)
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
		i := strings.Index(rankedOrder, term)
		if i <= last {
			t.Fatalf("term %q missing or out of order in %q", term, rankedOrder)
		}
		last = i
	}
}

// TestLeaderboardCursorRoundTrip performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestLeaderboardCursorRoundTrip(t *testing.T) {
	cursor := encodeLeaderboardCursor(125)
	offset, err := decodeLeaderboardCursor(cursor)
	if err != nil {
		t.Fatalf("decode cursor: %v", err)
	}
	if offset != 125 {
		t.Fatalf("offset = %d, want 125", offset)
	}
}

// TestLeaderboardCursorRejectsInvalidInput performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestLeaderboardCursorRejectsInvalidInput(t *testing.T) {
	if _, err := decodeLeaderboardCursor("not-base64"); err == nil {
		t.Fatal("invalid cursor was accepted")
	}
}

// TestCacheableLeaderboardOnlyDefaultTopRank performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestCacheableLeaderboardOnlyDefaultTopRank(t *testing.T) {
	if !cacheableLeaderboard(LeaderboardQuery{Limit: 50}) {
		t.Fatal("default top leaderboard should be cacheable")
	}
	if cacheableLeaderboard(LeaderboardQuery{Limit: 50, Sort: "p99"}) {
		t.Fatal("custom sorted leaderboard should not be cacheable")
	}
	if cacheableLeaderboard(LeaderboardQuery{Limit: 50, ContestantID: "c1"}) {
		t.Fatal("filtered leaderboard should not be cacheable")
	}
	if !cacheableLeaderboard(LeaderboardQuery{Limit: 50, Scenario: "constant"}) {
		t.Fatal("scenario-only leaderboard should be cacheable")
	}
}

// TestLeaderboardCacheKeyIncludesScenario ensures per-scenario boards do not
// collide in cache: distinct scenarios must produce distinct keys, and the
// empty scenario stays on the shared "all" key.
func TestLeaderboardCacheKeyIncludesScenario(t *testing.T) {
	all := leaderboardCacheKey(LeaderboardQuery{Limit: 50})
	constant := leaderboardCacheKey(LeaderboardQuery{Limit: 50, Scenario: "constant"})
	ramp := leaderboardCacheKey(LeaderboardQuery{Limit: 50, Scenario: "ramp"})
	if all == constant || constant == ramp {
		t.Fatalf("scenario keys must differ: all=%q constant=%q ramp=%q", all, constant, ramp)
	}
	if all != leaderboardCacheKey(LeaderboardQuery{Limit: 50, Scenario: ""}) {
		t.Fatal("empty scenario must map to the shared aggregate key")
	}
}

// TestIsUndefinedTable performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestIsUndefinedTable(t *testing.T) {
	if !isUndefinedTable(&pgconn.PgError{Code: "42P01"}) {
		t.Fatal("undefined_table was not recognized")
	}
	if isUndefinedTable(&pgconn.PgError{Code: "23505"}) {
		t.Fatal("non-undefined table error was recognized")
	}
}
