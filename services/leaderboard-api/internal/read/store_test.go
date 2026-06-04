package read

import (
	"strings"
	"testing"
)

func TestLeaderboardOrderByAllowlist(t *testing.T) {
	got := leaderboardOrderBy("p99", "desc")
	if !strings.HasPrefix(got, "p99_at_peak_ns DESC") {
		t.Fatalf("order = %q", got)
	}

	injected := leaderboardOrderBy("p99;DROP TABLE scores", "desc;DROP")
	if strings.Contains(injected, ";") || !strings.HasPrefix(injected, "rank ASC") {
		t.Fatalf("unsafe fallback order = %q", injected)
	}
}

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

func TestLeaderboardCursorRejectsInvalidInput(t *testing.T) {
	if _, err := decodeLeaderboardCursor("not-base64"); err == nil {
		t.Fatal("invalid cursor was accepted")
	}
}

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
}
