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

type fakeReader struct{}
type errorReader struct{ fakeReader }
type captureReader struct {
	fakeReader
	query read.LeaderboardQuery
}

func (fakeReader) Leaderboard(context.Context, read.LeaderboardQuery) (read.LeaderboardResponse, error) {
	return read.LeaderboardResponse{Source: "frozen", Rows: []read.LeaderboardRow{{Rank: 1, RunGroupID: "rg"}}}, nil
}
func (fakeReader) RunDetail(context.Context, string) (read.RunDetail, error) {
	return read.RunDetail{RunGroupID: "rg"}, nil
}
func (fakeReader) Chart(context.Context, string) ([]read.MetricPoint, error) {
	return []read.MetricPoint{{WaveIndex: 1}}, nil
}
func (fakeReader) ActiveRuns(context.Context) ([]read.ActiveRun, error) {
	return []read.ActiveRun{{RunGroupID: "rg1", TeamName: "T", Sessions: []read.ActiveSession{{SessionID: "s1", Scenario: "constant", Status: "running"}}}}, nil
}
func (errorReader) Leaderboard(context.Context, read.LeaderboardQuery) (read.LeaderboardResponse, error) {
	return read.LeaderboardResponse{}, errors.New(`bad "quoted" error`)
}
func (r *captureReader) Leaderboard(_ context.Context, q read.LeaderboardQuery) (read.LeaderboardResponse, error) {
	r.query = q
	return read.LeaderboardResponse{Source: "frozen"}, nil
}

func TestLeaderboardHandler(t *testing.T) {
	h := New(fakeReader{}, "http://prom")
	rec := httptest.NewRecorder()
	h.Leaderboard(rec, httptest.NewRequest("GET", "/api/leaderboard", nil))
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"run_group_id":"rg"`) {
		t.Fatalf("bad response: %d %s", rec.Code, rec.Body.String())
	}
}

func TestWriteJSONEscapesErrors(t *testing.T) {
	h := New(errorReader{}, "http://prom")
	rec := httptest.NewRecorder()
	h.Leaderboard(rec, httptest.NewRequest("GET", "/api/leaderboard", nil))
	if rec.Code != http.StatusInternalServerError || !strings.Contains(rec.Body.String(), `"error":"bad \"quoted\" error"`) {
		t.Fatalf("bad error response: %d %s", rec.Code, rec.Body.String())
	}
}

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
