// Package handler defines tests for benchmark ownership integration test.
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
	"github.com/iicpc/schemas/topics"
	"github.com/iicpc/submission-api/internal/publisher"
	"github.com/iicpc/submission-api/internal/store"
)

// recordingPublisher groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type recordingPublisher struct {
	benchmarks []publisher.BenchmarkMeta
}

// PublishBuildRequested applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (p *recordingPublisher) PublishBuildRequested(ctx context.Context, meta publisher.PublishMeta) error {
	return nil
}

// PublishBenchmarkRequested applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (p *recordingPublisher) PublishBenchmarkRequested(ctx context.Context, meta publisher.BenchmarkMeta) error {
	p.benchmarks = append(p.benchmarks, meta)
	return nil
}

// Close applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (p *recordingPublisher) Close() error { return nil }

// TestIntegration_StartBenchmarkClaimAndOwnership performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestIntegration_StartBenchmarkClaimAndOwnership(t *testing.T) {
	dsn := strings.TrimSpace(os.Getenv("DATABASE_URL"))
	if dsn == "" {
		t.Skip("set DATABASE_URL to run the benchmark ownership integration test")
	}
	ctx := context.Background()
	pg, err := store.NewPostgresStore(ctx, dsn)
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	defer pg.Close()

	if err := pg.SeedScenarios(ctx, []store.ScenarioRow{{
		ScenarioID: "sc-itest-ownership",
		Name:       "constant",
		SortOrder:  1,
		DurationNs: uint64(time.Second),
		TaskSpecs:  []topics.TaskSpec{{TaskID: 1, Profile: "hft", TargetRPS: 10, DurationNs: uint64(time.Second)}},
	}}, false); err != nil {
		t.Fatalf("seed scenarios: %v", err)
	}

	signer := newTokenSigner(t)
	pub := &recordingPublisher{}
	router := chi.NewRouter()
	router.Use(RequireContestant(signer.verifier(t), slog.Default()))
	router.Post("/submissions/{submission_id}/benchmark", StartBenchmark(pg, pub, slog.Default()))

	insertReady := func(t *testing.T, contestantID string) string {
		t.Helper()
		id := fmt.Sprintf("sub-bench-own-%d", time.Now().UnixNano())
		if err := pg.Insert(ctx, store.SubmissionMeta{
			SubmissionID: id, ContestantID: contestantID, SHA256: id, Language: "go",
			Protocol: "FIX", Port: 9898, TeamName: "t", ArtifactPath: "/x",
			ImageRef: "registry.local/sub:" + id, Status: topics.StatusReady,
			CreatedAt: time.Now().UTC(),
		}); err != nil {
			t.Fatalf("insert: %v", err)
		}
		return id
	}

	unowned := insertReady(t, "")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, signer.request(t, http.MethodPost, "/submissions/"+unowned+"/benchmark", "claimer"))
	if rec.Code != http.StatusAccepted {
		t.Fatalf("claim + start: got %d, want 202; body=%s", rec.Code, rec.Body.String())
	}
	meta, err := pg.GetByID(ctx, unowned)
	if err != nil {
		t.Fatalf("get after claim: %v", err)
	}
	if meta.ContestantID != "claimer" {
		t.Fatalf("contestant_id after claim = %q, want claimer", meta.ContestantID)
	}

	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, signer.request(t, http.MethodPost, "/submissions/"+unowned+"/benchmark", "rival"))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("rival on claimed submission: got %d, want 404; body=%s", rec.Code, rec.Body.String())
	}

	preclaimed := insertReady(t, "")
	if claimed, err := pg.ClaimSubmissionContestantIfEmpty(ctx, preclaimed, "early-bird"); err != nil || !claimed {
		t.Fatalf("pre-claim: claimed=%v err=%v, want true/nil", claimed, err)
	}
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, signer.request(t, http.MethodPost, "/submissions/"+preclaimed+"/benchmark", "latecomer"))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("latecomer on pre-claimed submission: got %d, want 404; body=%s", rec.Code, rec.Body.String())
	}

	if len(pub.benchmarks) == 0 {
		t.Fatal("no benchmark.requested published for the successful claim")
	}
	for _, b := range pub.benchmarks {
		if b.ContestantID != "claimer" {
			t.Fatalf("published contestant_id = %q, want claimer only", b.ContestantID)
		}
	}
}
