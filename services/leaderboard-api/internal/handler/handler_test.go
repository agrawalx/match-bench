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

func TestLeaderboardHandlerPassesSortOptions(t *testing.T) {
	reader := &captureReader{}
	h := New(reader, "http://prom")
	rec := httptest.NewRecorder()
	h.Leaderboard(rec, httptest.NewRequest("GET", "/api/leaderboard?limit=25&sort=p99&order=desc", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("bad response: %d %s", rec.Code, rec.Body.String())
	}
	if reader.query.Limit != 25 || reader.query.Sort != "p99" || reader.query.Order != "desc" {
		t.Fatalf("query = %#v", reader.query)
	}
}
