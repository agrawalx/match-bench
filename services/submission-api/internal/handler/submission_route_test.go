// Package handler defines tests for submission route test.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package handler

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/iicpc/submission-api/internal/store"
)

// TestIntegration_GetSubmissionRoute performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestIntegration_GetSubmissionRoute(t *testing.T) {
	dsn := strings.TrimSpace(os.Getenv("DATABASE_URL"))
	if dsn == "" {
		t.Skip("set DATABASE_URL to run the submission route integration test")
	}
	ctx := context.Background()
	pg, err := store.NewPostgresStore(ctx, dsn)
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	defer pg.Close()

	id := fmt.Sprintf("sub-itest-%d", time.Now().UnixNano())
	if err := pg.Insert(ctx, store.SubmissionMeta{
		SubmissionID: id, ContestantID: "c", SHA256: id, Language: "go",
		Protocol: "FIX", Port: 9898, TeamName: "t", ArtifactPath: "/x",
		Status: "ready", CreatedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("insert: %v", err)
	}

	signer := newTokenSigner(t)
	authMW := RequireContestant(signer.verifier(t), slog.Default())
	h := GetSubmission(pg, slog.Default())

	buggy := chi.NewRouter()
	buggy.Get("/submissions/{id}", h)
	rec := httptest.NewRecorder()
	buggy.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/submissions/"+id, nil))
	if rec.Code != http.StatusBadRequest {
		t.Errorf("mismatched route param: got %d, want 400 (documents the H12 bug)", rec.Code)
	}

	fixed := chi.NewRouter()
	fixed.Use(authMW)
	fixed.Get("/submissions/{submission_id}", h)
	rec = httptest.NewRecorder()
	fixed.ServeHTTP(rec, signer.request(t, http.MethodGet, "/submissions/"+id, "c"))
	if rec.Code != http.StatusOK {
		t.Fatalf("matched route, existing submission: got %d, want 200; body=%s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	fixed.ServeHTTP(rec, forgedRequest(http.MethodGet, "/submissions/"+id, "c"))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("forged alg:none token: got %d, want 401 (Critical-1 regression)", rec.Code)
	}

	rec = httptest.NewRecorder()
	fixed.ServeHTTP(rec, signer.request(t, http.MethodGet, "/submissions/"+id, "other-contestant"))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("matched route, other contestant: got %d, want 404; body=%s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	fixed.ServeHTTP(rec, signer.request(t, http.MethodGet, "/submissions/does-not-exist-"+id, "c"))
	if rec.Code != http.StatusNotFound {
		t.Errorf("matched route, missing submission: got %d, want 404", rec.Code)
	}
}
