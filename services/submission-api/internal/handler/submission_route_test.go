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

// TestIntegration_GetSubmissionRoute reproduces H12: the chi route param name
// MUST match the key the handler reads via chi.URLParam. With the mismatched
// route ("{id}" vs handler key "submission_id") every request hit the empty-id
// guard and returned 400, so status polling was entirely non-functional. The
// matched route returns 200 for an existing submission and 404 for a missing one.
//
// Post-Critical-1 it also pins the auth contract on this route: requests ride
// through RequireContestant, so a forged alg:none token — which the pre-fix
// contestantIDFromRequest accepted — is rejected with 401 before the handler
// runs, and only properly signed tokens reach the ownership check.
//
// Env-gated: needs a live Postgres (DATABASE_URL).
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

	// The original (buggy) route param name -> handler reads "" -> 400.
	// No auth middleware here on purpose: the 400 fires on the empty route
	// param before any contestant check, documenting the H12 bug in isolation.
	buggy := chi.NewRouter()
	buggy.Get("/submissions/{id}", h)
	rec := httptest.NewRecorder()
	buggy.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/submissions/"+id, nil))
	if rec.Code != http.StatusBadRequest {
		t.Errorf("mismatched route param: got %d, want 400 (documents the H12 bug)", rec.Code)
	}

	// The fixed route param name matches the handler -> 200 for an existing id.
	fixed := chi.NewRouter()
	fixed.Use(authMW)
	fixed.Get("/submissions/{submission_id}", h)
	rec = httptest.NewRecorder()
	fixed.ServeHTTP(rec, signer.request(t, http.MethodGet, "/submissions/"+id, "c"))
	if rec.Code != http.StatusOK {
		t.Fatalf("matched route, existing submission: got %d, want 200; body=%s", rec.Code, rec.Body.String())
	}

	// The Critical-1 attack: a forged alg:none token naming the owner's sub.
	// Pre-fix this returned 200 (full impersonation); it must now be 401.
	rec = httptest.NewRecorder()
	fixed.ServeHTTP(rec, forgedRequest(http.MethodGet, "/submissions/"+id, "c"))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("forged alg:none token: got %d, want 401 (Critical-1 regression)", rec.Code)
	}

	// A correctly signed token for a DIFFERENT contestant: authenticated but
	// not the owner -> 404, not 403, to avoid existence leaks.
	rec = httptest.NewRecorder()
	fixed.ServeHTTP(rec, signer.request(t, http.MethodGet, "/submissions/"+id, "other-contestant"))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("matched route, other contestant: got %d, want 404; body=%s", rec.Code, rec.Body.String())
	}

	// Missing id on the fixed route -> 404 (not 400/500).
	rec = httptest.NewRecorder()
	fixed.ServeHTTP(rec, signer.request(t, http.MethodGet, "/submissions/does-not-exist-"+id, "c"))
	if rec.Code != http.StatusNotFound {
		t.Errorf("matched route, missing submission: got %d, want 404", rec.Code)
	}
}
