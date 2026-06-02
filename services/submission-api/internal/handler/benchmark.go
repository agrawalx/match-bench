package handler

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/iicpc/libs/metrics"
	"github.com/iicpc/schemas/topics"
	cerrs "github.com/iicpc/submission-api/internal/errors"
	"github.com/iicpc/submission-api/internal/publisher"
	"github.com/iicpc/submission-api/internal/store"
)

// runGroupChild is one row in the benchmarkResponse.Runs array — one entry
// per scenario the controller will execute back-to-back.
type runGroupChild struct {
	SessionID    string    `json:"session_id"`
	ScenarioID   string    `json:"scenario_id"`
	ScenarioName string    `json:"scenario_name"`
	Status       string    `json:"status"`
	Message      string    `json:"message,omitempty"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

type benchmarkResponse struct {
	RunGroupID   string          `json:"run_group_id"`
	SubmissionID string          `json:"submission_id"`
	Status       string          `json:"status"` // run-group's status (requested | running | completed | failed)
	CreatedAt    time.Time       `json:"created_at"`
	Runs         []runGroupChild `json:"runs"`
}

type runResponse struct {
	SessionID    string    `json:"session_id"`
	SubmissionID string    `json:"submission_id"`
	RunGroupID   string    `json:"run_group_id,omitempty"`
	ScenarioID   string    `json:"scenario_id,omitempty"`
	Status       string    `json:"status"`
	Message      string    `json:"message"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

// StartBenchmark handles POST /submissions/{submission_id}/benchmark.
//
// One click expands into a run-group with N child sessions, one per row in
// the scenarios table (v1: constant, spike, ramp). The handler mints one
// run_group_id and N session_ids, persists everything in a single transaction,
// and publishes N benchmark.requested messages — each carrying the shared
// run_group_id and its own scenario_id.
//
// Idempotency contract (unchanged from the per-run version):
//   - HTTP 200 + existing run_group_id  → an active run-group is already in
//     flight for this submission; caller joined it.
//   - HTTP 202 + new run_group_id       → a fresh run-group was started.
//
// Body shape is identical in both cases; the status code distinguishes
// "joined" from "started".
//
// Idempotency is enforced at the database level by a partial unique index
// on run_groups(submission_id) WHERE status NOT IN ('completed','failed').
// The flow is:
//
//  1. Look up the submission; verify status='ready'.
//  2. List scenarios. If empty, return 500 — the seed must have run.
//  3. Check for an active run-group for this submission. If found,
//     load its child runs and return 200.
//  4. Mint run_group_id + N session_ids. INSERT all rows in one transaction.
//  5. On unique_violation (lost a race with a concurrent click) → re-query
//     the active group and return whichever row won, with 200.
//  6. Publish one benchmark.requested per child. Each carries the same
//     run_group_id; each carries a distinct scenario_id.
//  7. Return 202 with the freshly minted run_group_id and child sessions.
//
// session_id and run_group_id are minted here as UUID v7 — globally unique
// and time-ordered (v7 timestamps embed in the high bits). The API returns
// them synchronously so the frontend can start tracking immediately.
//
// Failure-mode note: if the INSERT succeeds but a subsequent Kafka publish
// fails, we have an orphaned run-group with some scenarios announced and
// some not. The runs row is already inserted but Kafka publish failed —
// we can't roll back the INSERT (another reader could already be looking
// at it). The bot-fleet-controller's startup recovery sweep marks every
// in-flight run failed on its next restart, so this orphan cleans itself
// up. We surface the publish failure to the user so they retry.
func StartBenchmark(pg *store.PostgresStore, pub publisher.Publisher, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		submissionID := chi.URLParam(r, "submission_id")
		if submissionID == "" {
			writeError(w, http.StatusBadRequest, "missing submission_id")
			return
		}

		ctx := r.Context()

		sub, err := pg.GetByID(ctx, submissionID)
		if err != nil {
			log.ErrorContext(ctx, "get submission", "submission_id", submissionID, "error", err)
			writeError(w, http.StatusInternalServerError, "lookup failed")
			return
		}
		if sub == nil {
			writeError(w, http.StatusNotFound, "submission not found")
			return
		}
		if sub.Status != topics.StatusReady {
			writeError(w, http.StatusBadRequest, "submission is not in 'ready' status (current: "+sub.Status+")")
			return
		}

		scenarios, err := pg.ListScenarios(ctx)
		if err != nil {
			log.ErrorContext(ctx, "list scenarios", "error", err)
			writeError(w, http.StatusInternalServerError, "scenario lookup failed")
			return
		}
		if len(scenarios) == 0 {
			// Defensive: the scenarios table is seeded on submission-api
			// startup. An empty table means the seed didn't run or someone
			// deleted everything — either way, we cannot proceed.
			log.ErrorContext(ctx, "scenarios table is empty — seed must run before benchmarks can be triggered")
			writeError(w, http.StatusInternalServerError, "no scenarios configured")
			return
		}

		// Step 3 of the flow above — short-circuit on existing active group.
		if existing, err := pg.FindActiveRunGroup(ctx, submissionID); err != nil {
			log.ErrorContext(ctx, "find active run-group", "submission_id", submissionID, "error", err)
			writeError(w, http.StatusInternalServerError, "lookup failed")
			return
		} else if existing != nil {
			metrics.Counter("benchmark_requests_total", "Benchmark requests by result.", metrics.Labels("result", "joined_existing"), 1)
			metrics.Counter("active_run_group_conflicts_total", "Benchmark requests that joined an existing active run-group.", nil, 1)
			respondWithGroup(ctx, w, pg, log, existing, scenarios, http.StatusOK)
			return
		}

		runGroupID, err := newUUIDv7()
		if err != nil {
			writeError(w, http.StatusInternalServerError, "failed to generate run_group_id")
			return
		}
		now := time.Now().UTC()
		group := store.RunGroupMeta{
			RunGroupID:   runGroupID,
			SubmissionID: submissionID,
			ContestantID: sub.ContestantID,
			Status:       topics.RunStatusRequested,
			CreatedAt:    now,
			UpdatedAt:    now,
		}

		children := make([]store.RunMeta, 0, len(scenarios))
		for _, sc := range scenarios {
			sessionID, err := newUUIDv7()
			if err != nil {
				writeError(w, http.StatusInternalServerError, "failed to generate session_id")
				return
			}
			children = append(children, store.RunMeta{
				SessionID:    sessionID,
				SubmissionID: submissionID,
				ContestantID: sub.ContestantID,
				RunGroupID:   runGroupID,
				ScenarioID:   sc.ScenarioID,
				Status:       topics.RunStatusRequested,
				Message:      "",
				CreatedAt:    now,
				UpdatedAt:    now,
			})
		}

		if err := pg.InsertRunGroupWithChildren(ctx, group, children); err != nil {
			if errors.Is(err, cerrs.ErrActiveRunGroupExists) {
				// Lost a race with a concurrent click — another request
				// inserted an active run_groups row for this submission
				// between our FindActiveRunGroup check above and our INSERT
				// here. The partial unique index rejected ours with
				// unique_violation (PostgreSQL error code 23505), which the
				// store layer translated to ErrActiveRunGroupExists.
				// Re-query and return whichever group won, with HTTP 200.
				existing, ferr := pg.FindActiveRunGroup(ctx, submissionID)
				if ferr != nil || existing == nil {
					log.ErrorContext(ctx, "active run-group conflict but no row found", "submission_id", submissionID, "error", ferr)
					writeError(w, http.StatusInternalServerError, "concurrent benchmark request conflict")
					return
				}
				metrics.Counter("benchmark_requests_total", "Benchmark requests by result.", metrics.Labels("result", "race_joined_existing"), 1)
				metrics.Counter("active_run_group_conflicts_total", "Benchmark requests that joined an existing active run-group.", nil, 1)
				respondWithGroup(ctx, w, pg, log, existing, scenarios, http.StatusOK)
				return
			}
			log.ErrorContext(ctx, "insert run-group", "submission_id", submissionID, "error", err)
			writeError(w, http.StatusInternalServerError, "failed to create run-group")
			return
		}

		// Publish one benchmark.requested per child. They are independent;
		// the controller processes them sequentially per run-group (see the
		// run-group orchestrator). Kafka ordering across these N messages
		// is not required because the controller drains them via PostgreSQL
		// queries, not Kafka offsets.
		for _, c := range children {
			if err := pub.PublishBenchmarkRequested(ctx, publisher.BenchmarkMeta{
				SessionID:    c.SessionID,
				SubmissionID: c.SubmissionID,
				ContestantID: c.ContestantID,
				RunGroupID:   c.RunGroupID,
				ScenarioID:   c.ScenarioID,
				RequestedAt:  now,
			}); err != nil {
				// Orphan window — see the failure-mode note in the function
				// doc. Surface the failure so the user can retry; the
				// controller's startup recovery sweep will clean up the
				// half-published group on its next restart.
				log.ErrorContext(ctx, "publish benchmark.requested",
					"session_id", c.SessionID, "scenario_id", c.ScenarioID, "error", err)
				metrics.Counter("benchmark_publish_failures_total", "Benchmark publish failures by topic.", metrics.Labels("topic", topics.TopicBenchmarkRequested), 1)
				metrics.Counter("benchmark_requests_total", "Benchmark requests by result.", metrics.Labels("result", "publish_failed"), 1)
				writeError(w, http.StatusInternalServerError, "failed to publish benchmark request")
				return
			}
		}

		log.InfoContext(ctx, "benchmark requested",
			"submission_id", submissionID,
			"run_group_id", runGroupID,
			"scenarios", len(children))
		metrics.Counter("benchmark_requests_total", "Benchmark requests by result.", metrics.Labels("result", "started"), 1)
		metrics.Counter("run_groups_created_total", "Run-groups created by submission-api.", nil, 1)
		for _, sc := range scenarios {
			metrics.Counter("run_group_children_created_total", "Child runs created by scenario.", metrics.Labels("scenario_name", sc.Name), 1)
		}
		respondWithGroup(ctx, w, pg, log, &group, scenarios, http.StatusAccepted)
	}
}

// GetRunGroup handles GET /run-groups/{run_group_id}.
// Returns the group's metadata plus all child runs in scenario-name order.
func GetRunGroup(pg *store.PostgresStore, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		runGroupID := chi.URLParam(r, "run_group_id")
		if runGroupID == "" {
			writeError(w, http.StatusBadRequest, "missing run_group_id")
			return
		}
		ctx := r.Context()
		group, err := pg.GetRunGroup(ctx, runGroupID)
		if err != nil {
			log.ErrorContext(ctx, "get run-group", "run_group_id", runGroupID, "error", err)
			writeError(w, http.StatusInternalServerError, "lookup failed")
			return
		}
		if group == nil {
			writeError(w, http.StatusNotFound, "run-group not found")
			return
		}
		scenarios, err := pg.ListScenarios(ctx)
		if err != nil {
			log.ErrorContext(ctx, "list scenarios", "error", err)
			writeError(w, http.StatusInternalServerError, "scenario lookup failed")
			return
		}
		respondWithGroup(ctx, w, pg, log, group, scenarios, http.StatusOK)
	}
}

// GetRun handles GET /runs/{session_id}.
// Single-session view for the frontend's per-scenario tab.
func GetRun(pg *store.PostgresStore, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		sessionID := chi.URLParam(r, "session_id")
		if sessionID == "" {
			writeError(w, http.StatusBadRequest, "missing session_id")
			return
		}
		run, err := pg.GetRun(r.Context(), sessionID)
		if err != nil {
			log.ErrorContext(r.Context(), "get run", "session_id", sessionID, "error", err)
			writeError(w, http.StatusInternalServerError, "lookup failed")
			return
		}
		if run == nil {
			writeError(w, http.StatusNotFound, "run not found")
			return
		}
		writeJSON(w, http.StatusOK, runResponse{
			SessionID:    run.SessionID,
			SubmissionID: run.SubmissionID,
			RunGroupID:   run.RunGroupID,
			ScenarioID:   run.ScenarioID,
			Status:       run.Status,
			Message:      run.Message,
			CreatedAt:    run.CreatedAt,
			UpdatedAt:    run.UpdatedAt,
		})
	}
}

// respondWithGroup loads child runs for the group, joins them with scenarios
// to produce human-readable names, and writes the JSON response with the
// given HTTP status. Used by both the create path (HTTP 202), the
// already-exists path (HTTP 200), and GET /run-groups/{id} (HTTP 200).
func respondWithGroup(
	ctx context.Context,
	w http.ResponseWriter,
	pg *store.PostgresStore,
	log *slog.Logger,
	group *store.RunGroupMeta,
	scenarios []store.ScenarioRow,
	httpStatus int,
) {
	scenarioName := make(map[string]string, len(scenarios))
	for _, sc := range scenarios {
		scenarioName[sc.ScenarioID] = sc.Name
	}

	runs, err := pg.ListRunsByGroup(ctx, group.RunGroupID)
	if err != nil {
		log.ErrorContext(ctx, "list runs by group", "run_group_id", group.RunGroupID, "error", err)
		writeError(w, http.StatusInternalServerError, "failed to load child runs")
		return
	}
	children := make([]runGroupChild, 0, len(runs))
	for _, r := range runs {
		children = append(children, runGroupChild{
			SessionID:    r.SessionID,
			ScenarioID:   r.ScenarioID,
			ScenarioName: scenarioName[r.ScenarioID],
			Status:       r.Status,
			Message:      r.Message,
			CreatedAt:    r.CreatedAt,
			UpdatedAt:    r.UpdatedAt,
		})
	}
	writeJSON(w, httpStatus, benchmarkResponse{
		RunGroupID:   group.RunGroupID,
		SubmissionID: group.SubmissionID,
		Status:       group.Status,
		CreatedAt:    group.CreatedAt,
		Runs:         children,
	})
}

func newUUIDv7() (string, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return "", err
	}
	return id.String(), nil
}
