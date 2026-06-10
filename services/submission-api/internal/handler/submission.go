// Package handler implements submission behavior.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package handler

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/iicpc/submission-api/internal/store"
)

// submissionResponse groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type submissionResponse struct {
	SubmissionID string    `json:"submission_id"`
	Status       string    `json:"status"`
	Language     string    `json:"language"`
	Protocol     string    `json:"protocol"`
	Port         int       `json:"port"`
	TeamName     string    `json:"team_name"`
	SHA256       string    `json:"sha256"`
	ImageRef     string    `json:"image_ref"`
	CreatedAt    time.Time `json:"created_at"`
}

// GetSubmission performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func GetSubmission(pg *store.PostgresStore, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
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
		contestantID := contestantIDFromContext(r.Context())
		if contestantID == "" {
			writeError(w, http.StatusUnauthorized, "authentication required")
			return
		}
		if meta.ContestantID != "" && meta.ContestantID != contestantID {
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
			ImageRef:     meta.ImageRef,
			CreatedAt:    meta.CreatedAt,
		})
	}
}
