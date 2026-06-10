// Package handler defines tests for handler test.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package handler

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/iicpc/leaderboard-api/internal/read"
)

// fakeReader groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type fakeReader struct{}

// errorReader groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type errorReader struct{ fakeReader }

// captureReader groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type captureReader struct {
	fakeReader
	query read.LeaderboardQuery
}

// Leaderboard applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (fakeReader) Leaderboard(context.Context, read.LeaderboardQuery) (read.LeaderboardResponse, error) {
	return read.LeaderboardResponse{Source: "frozen", Rows: []read.LeaderboardRow{{Rank: 1, RunGroupID: "rg"}}}, nil
}

// RunDetail applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (fakeReader) RunDetail(context.Context, string) (read.RunDetail, error) {
	return read.RunDetail{RunGroupID: "rg"}, nil
}

// Chart applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (fakeReader) Chart(context.Context, string) ([]read.MetricPoint, error) {
	return []read.MetricPoint{{WaveIndex: 1}}, nil
}

// ActiveRuns applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (fakeReader) ActiveRuns(context.Context) ([]read.ActiveRun, error) {
	return []read.ActiveRun{{RunGroupID: "rg1", TeamName: "T", Sessions: []read.ActiveSession{{SessionID: "s1", Scenario: "constant", Status: "running"}}}}, nil
}

// Leaderboard applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (errorReader) Leaderboard(context.Context, read.LeaderboardQuery) (read.LeaderboardResponse, error) {
	return read.LeaderboardResponse{}, errors.New(`bad "quoted" error`)
}

// Leaderboard applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (r *captureReader) Leaderboard(_ context.Context, q read.LeaderboardQuery) (read.LeaderboardResponse, error) {
	r.query = q
	return read.LeaderboardResponse{Source: "frozen"}, nil
}

// TestLeaderboardHandler performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestLeaderboardHandler(t *testing.T) {
	h := New(fakeReader{}, "http://prom")
	rec := httptest.NewRecorder()
	h.Leaderboard(rec, httptest.NewRequest("GET", "/api/leaderboard", nil))
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"run_group_id":"rg"`) {
		t.Fatalf("bad response: %d %s", rec.Code, rec.Body.String())
	}
}

// TestWriteJSONEscapesErrors performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestWriteJSONEscapesErrors(t *testing.T) {
	h := New(errorReader{}, "http://prom")
	rec := httptest.NewRecorder()
	h.Leaderboard(rec, httptest.NewRequest("GET", "/api/leaderboard", nil))
	if rec.Code != http.StatusInternalServerError || !strings.Contains(rec.Body.String(), `"error":"bad \"quoted\" error"`) {
		t.Fatalf("bad error response: %d %s", rec.Code, rec.Body.String())
	}
}

// TestLeaderboardHandlerPassesQueryOptions performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestLeaderboardHandlerPassesQueryOptions(t *testing.T) {
	reader := &captureReader{}
	h := New(reader, "http://prom")
	rec := httptest.NewRecorder()
	h.Leaderboard(rec, httptest.NewRequest("GET", "/api/leaderboard?limit=25&sort=p99&order=desc&cursor=abc&contestant_id=c1&team_id=t1&submission_id=s1&run_group_id=rg1&team_name=Alpha", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("bad response: %d %s", rec.Code, rec.Body.String())
	}
	if reader.query.Limit != 25 ||
		reader.query.Sort != "p99" ||
		reader.query.Order != "desc" ||
		reader.query.Cursor != "abc" ||
		reader.query.ContestantID != "c1" ||
		reader.query.TeamID != "t1" ||
		reader.query.SubmissionID != "s1" ||
		reader.query.RunGroupID != "rg1" ||
		reader.query.TeamName != "Alpha" {
		t.Fatalf("query = %#v", reader.query)
	}
}
