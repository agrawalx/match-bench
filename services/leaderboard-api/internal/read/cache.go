// Package read implements cache behavior.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package read

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/iicpc/libs/metrics"
)

// Cache defines the behavior expected by this package boundary.
// Implementations should preserve the caller-visible contract.
type Cache interface {
	Get(context.Context, string) ([]byte, bool, error)
	Set(context.Context, string, []byte, time.Duration) error
}

// CachedReader groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type CachedReader struct {
	base  *Store
	cache Cache
	ttl   time.Duration
}

// NewCached performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func NewCached(base *Store, cache Cache, ttl time.Duration) *CachedReader {
	return &CachedReader{base: base, cache: cache, ttl: ttl}
}

// Leaderboard applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (r *CachedReader) Leaderboard(ctx context.Context, q LeaderboardQuery) (LeaderboardResponse, error) {
	if !cacheableLeaderboard(q) || r.cache == nil || r.ttl <= 0 {
		return r.base.Leaderboard(ctx, q)
	}
	key := leaderboardCacheKey(q)
	if payload, ok, err := r.cache.Get(ctx, key); err == nil && ok {
		var resp LeaderboardResponse
		if err := json.Unmarshal(payload, &resp); err == nil {
			metrics.Counter("leaderboard_api_cache_reads_total", "Leaderboard cache reads by result.", metrics.Labels("result", "hit"), 1)
			return resp, nil
		}
	} else if err != nil {
		metrics.Counter("leaderboard_api_cache_reads_total", "Leaderboard cache reads by result.", metrics.Labels("result", "error"), 1)
	}
	metrics.Counter("leaderboard_api_cache_reads_total", "Leaderboard cache reads by result.", metrics.Labels("result", "miss"), 1)
	resp, err := r.base.Leaderboard(ctx, q)
	if err != nil {
		return resp, err
	}
	if payload, err := json.Marshal(resp); err == nil {
		if err := r.cache.Set(ctx, key, payload, r.ttl); err != nil {
			metrics.Counter("leaderboard_api_cache_writes_total", "Leaderboard cache writes by result.", metrics.Labels("result", "error"), 1)
		} else {
			metrics.Counter("leaderboard_api_cache_writes_total", "Leaderboard cache writes by result.", metrics.Labels("result", "ok"), 1)
		}
	}
	return resp, nil
}

// RunDetail applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (r *CachedReader) RunDetail(ctx context.Context, runGroupID string) (RunDetail, error) {
	return r.base.RunDetail(ctx, runGroupID)
}

// Chart applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (r *CachedReader) Chart(ctx context.Context, sessionID string) ([]MetricPoint, error) {
	return r.base.Chart(ctx, sessionID)
}

// ActiveRuns applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (r *CachedReader) ActiveRuns(ctx context.Context) ([]ActiveRun, error) {
	return r.base.ActiveRuns(ctx)
}

// cacheableLeaderboard performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func cacheableLeaderboard(q LeaderboardQuery) bool {
	sortField := strings.ToLower(strings.TrimSpace(q.Sort))
	order := strings.ToLower(strings.TrimSpace(q.Order))
	return q.Cursor == "" &&
		q.RunGroupID == "" &&
		q.SubmissionID == "" &&
		q.ContestantID == "" &&
		q.TeamID == "" &&
		q.TeamName == "" &&
		(sortField == "" || sortField == "rank") &&
		(order == "" || order == "asc")
}

// leaderboardCacheKey performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func leaderboardCacheKey(q LeaderboardQuery) string {
	limit := q.Limit
	if limit <= 0 || limit > maxLeaderboardRows {
		limit = defaultLeaderboardRows
	}
	return "leaderboard-api:v1:top:" + strconv.Itoa(limit)
}
