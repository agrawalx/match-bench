package handler

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/iicpc/submission-api/internal/store"
)

type submissionResponse struct {
	SubmissionID string    `json:"submission_id"`
	Status       string    `json:"status"`
	Language     string    `json:"language"`
	Protocol     string    `json:"protocol"`
	Port         int       `json:"port"`
	TeamName     string    `json:"team_name"`
	SHA256       string    `json:"sha256"`
	CreatedAt    time.Time `json:"created_at"`
}

func GetSubmission(pg *store.PostgresStore, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// TODO(auth): enforce contestant ownership before ContestantID becomes non-empty.
		submissionID := chi.URLParam(r, "submission_id")
		if submissionID == "" {
			writeError(w, http.StatusBadRequest, "missing submission id")
			return
		}

		meta, err := pg.GetByID(r.Context(), submissionID)
		if err != nil {
			log.ErrorContext(r.Context(), "get submission failed", "submission_id", submissionID, "error", err)
			writeError(w, http.StatusInternalServerError, "internal error")
			return
		}
		if meta == nil {
			writeError(w, http.StatusNotFound, "submission not found")
			return
		}

		writeJSON(w, http.StatusOK, submissionResponse{
			SubmissionID: meta.SubmissionID,
			Status:       meta.Status,
			Language:     meta.Language,
			Protocol:     meta.Protocol,
			Port:         meta.Port,
			TeamName:     meta.TeamName,
			SHA256:       meta.SHA256,
			CreatedAt:    meta.CreatedAt,
		})
	}
}
