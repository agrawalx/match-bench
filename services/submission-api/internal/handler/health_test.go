package handler

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestReadiness reproduces L35: readiness must reflect datastore health, not a
// static 200. A failing ping -> 503; a healthy ping -> 200.
func TestReadiness(t *testing.T) {
	down := Readiness(func(context.Context) error { return errors.New("pool closed") }, slog.Default())
	rec := httptest.NewRecorder()
	down(rec, httptest.NewRequest(http.MethodGet, "/ready", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("ping failure: got %d, want 503", rec.Code)
	}

	up := Readiness(func(context.Context) error { return nil }, slog.Default())
	rec = httptest.NewRecorder()
	up(rec, httptest.NewRequest(http.MethodGet, "/ready", nil))
	if rec.Code != http.StatusOK {
		t.Errorf("healthy ping: got %d, want 200", rec.Code)
	}
}
