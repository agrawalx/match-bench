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

// recordingPublisher satisfies publisher.Publisher without Kafka — the
// ownership tests only care about HTTP semantics, not delivery.
type recordingPublisher struct {
	benchmarks []publisher.BenchmarkMeta
}

func (p *recordingPublisher) PublishBuildRequested(ctx context.Context, meta publisher.PublishMeta) error {
	return nil
}

func (p *recordingPublisher) PublishBenchmarkRequested(ctx context.Context, meta publisher.BenchmarkMeta) error {
	p.benchmarks = append(p.benchmarks, meta)
	return nil
}

func (p *recordingPublisher) Close() error { return nil }

// TestIntegration_StartBenchmarkClaimAndOwnership pins the StartBenchmark
// claim/ownership call site against a live database:
//
//  1. an unowned (contestant_id = ”) ready submission is atomically claimed
//     by the first authenticated requester (202, row bound in the DB);
//  2. a second contestant hitting the now-owned submission gets 404 — the DB
//     row, not the request, decides ownership;
//  3. a submission pre-claimed by someone else (the lost-race shape: the row
//     was bound between lookups) is also 404 for everyone else.
//
// Env-gated: needs a live Postgres (DATABASE_URL).
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

	// StartBenchmark refuses to run with an empty scenarios table; seed one
	// canonical row (idempotent ON CONFLICT DO NOTHING, safe on a shared DB).
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

	// 1. Unowned submission: first authenticated requester claims and starts.
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

	// 2. Same submission, different contestant: the DB row says 'claimer',
	// so 'rival' must see 404 — not join or restart the run.
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, signer.request(t, http.MethodPost, "/submissions/"+unowned+"/benchmark", "rival"))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("rival on claimed submission: got %d, want 404; body=%s", rec.Code, rec.Body.String())
	}

	// 3. The lost-race shape: a row bound to another contestant before our
	// request reads it (here: pre-claimed via the store) is invisible to us.
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
