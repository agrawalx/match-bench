package handler

import (
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/iicpc/schemas/topics"
	cerrs "github.com/iicpc/submission-api/internal/errors"
	"github.com/iicpc/submission-api/internal/publisher"
	"github.com/iicpc/submission-api/internal/store"
)

type benchmarkResponse struct {
	RunID        string    `json:"run_id"`
	SubmissionID string    `json:"submission_id"`
	Status       string    `json:"status"`
	Message      string    `json:"message,omitempty"`
	CreatedAt    time.Time `json:"created_at"`
}

type runResponse struct {
	SessionID    string    `json:"session_id"`
	SubmissionID string    `json:"submission_id"`
	Status       string    `json:"status"`
	Message      string    `json:"message"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

// StartBenchmark handles POST /benchmarks/{submission_id}.
//
// Idempotency contract:
//   - HTTP 200 + existing run_id  → an active run is already in flight for
//     this submission; caller joined it.
//   - HTTP 202 + new run_id       → a fresh run was started.
//
// Body shape is identical in both cases; the status code distinguishes
// "joined" from "started".
//
// Idempotency is enforced at the database level by a partial unique index
// on runs(submission_id) WHERE status NOT IN ('completed','failed'). The
// flow below is:
//   1. Query runs for an active row for this submission.
//   2. If found → return 200 with existing run_id.
//   3. Else INSERT runs row with status='requested'.
//   4. On unique_violation (a concurrent click landed first) → re-query
//      and return whichever row won, with 200.
//   5. Else publish benchmark.requested and return 202.
//
// session_id is minted here as UUID v7 so it is both globally unique and
// time-ordered (v7 timestamps embed in the high bits). The API must
// return it synchronously so the frontend can start tracking immediately.
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

		// Idempotency check — step 1 of the flow documented above. If an
		// active run exists, return its run_id with HTTP 200 instead of
		// minting a new one.
		if existing, err := pg.FindActiveRun(ctx, submissionID); err != nil {
			log.ErrorContext(ctx, "find active run", "submission_id", submissionID, "error", err)
			writeError(w, http.StatusInternalServerError, "lookup failed")
			return
		} else if existing != nil {
			log.InfoContext(ctx, "returning existing active run", "submission_id", submissionID, "session_id", existing.SessionID, "status", existing.Status)
			writeJSON(w, http.StatusOK, benchmarkResponse{
				RunID:        existing.SessionID,
				SubmissionID: existing.SubmissionID,
				Status:       existing.Status,
				Message:      existing.Message,
				CreatedAt:    existing.CreatedAt,
			})
			return
		}

		id, err := uuid.NewV7()
		if err != nil {
			writeError(w, http.StatusInternalServerError, "failed to generate session id")
			return
		}
		sessionID := id.String()
		now := time.Now().UTC()

		run := store.RunMeta{
			SessionID:    sessionID,
			SubmissionID: submissionID,
			ContestantID: sub.ContestantID,
			Status:       topics.RunStatusRequested,
			Message:      "",
			CreatedAt:    now,
			UpdatedAt:    now,
		}
		if err := pg.InsertRun(ctx, run); err != nil {
			if errors.Is(err, cerrs.ErrActiveRunExists) {
				// Lost a race with a concurrent click — another request
				// inserted an active runs row for this submission between
				// our FindActiveRun check above and our INSERT here. The
				// partial unique index rejected ours with unique_violation
				// (PostgreSQL error code 23505), which the store layer
				// translated to ErrActiveRunExists. Re-query and return
				// whichever run won, with HTTP 200.
				existing, ferr := pg.FindActiveRun(ctx, submissionID)
				if ferr != nil || existing == nil {
					log.ErrorContext(ctx, "active run conflict but no row found", "submission_id", submissionID, "error", ferr)
					writeError(w, http.StatusInternalServerError, "concurrent benchmark request conflict")
					return
				}
				writeJSON(w, http.StatusOK, benchmarkResponse{
					RunID:        existing.SessionID,
					SubmissionID: existing.SubmissionID,
					Status:       existing.Status,
					Message:      existing.Message,
					CreatedAt:    existing.CreatedAt,
				})
				return
			}
			log.ErrorContext(ctx, "insert run", "submission_id", submissionID, "error", err)
			writeError(w, http.StatusInternalServerError, "failed to create run")
			return
		}

		if err := pub.PublishBenchmarkRequested(ctx, publisher.BenchmarkMeta{
			SessionID:    sessionID,
			SubmissionID: submissionID,
			ContestantID: sub.ContestantID,
			RequestedAt:  now,
		}); err != nil {
			// The runs row is already inserted but Kafka publish failed.
			// We can't roll back the INSERT (another request could already
			// be reading it). The controller is single-replica and runs a
			// crash-recovery sweep on startup that marks every in-flight
			// run failed — so this orphaned 'requested' row will be
			// cleaned up the next time the controller restarts, or it
			// will stall here until the controller (or its consumer)
			// eventually receives the message via a different mechanism.
			// Surface the publish failure to the user so they retry.
			log.ErrorContext(ctx, "publish benchmark.requested", "session_id", sessionID, "error", err)
			writeError(w, http.StatusInternalServerError, "failed to publish benchmark request")
			return
		}

		log.InfoContext(ctx, "benchmark requested", "submission_id", submissionID, "session_id", sessionID)
		writeJSON(w, http.StatusAccepted, benchmarkResponse{
			RunID:        sessionID,
			SubmissionID: submissionID,
			Status:       topics.RunStatusRequested,
			CreatedAt:    now,
		})
	}
}

// GetRun handles GET /runs/{session_id}.
// The frontend polls (or SSE-streams later) this endpoint to track progress.
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
			Status:       run.Status,
			Message:      run.Message,
			CreatedAt:    run.CreatedAt,
			UpdatedAt:    run.UpdatedAt,
		})
	}
}
