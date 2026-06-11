// Package handler defines tests for health test.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package handler

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestReadiness performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
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
