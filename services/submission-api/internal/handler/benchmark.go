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
// Idempotent: returns 200 with the existing run_id when an active run is
// already in flight for this submission, otherwise 202 with a freshly minted
// session_id. See CONVENTIONS.md §11.
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

		// Idempotency check (Layer 1, see CONVENTIONS.md §11).
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
				// Lost a race with a concurrent click. Fall back to returning
				// whichever run won (Layer 1, step 4 in CONVENTIONS.md §11).
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
			// The runs row is already inserted. The controller's startup
			// recovery (CONVENTIONS.md §9) will mark this run failed if the
			// controller never sees it. Surface the publish failure to the
			// user so they know to retry.
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
