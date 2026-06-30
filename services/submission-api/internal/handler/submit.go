// Package handler implements submit behavior.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package handler

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/iicpc/libs/metrics"
	cerrs "github.com/iicpc/submission-api/internal/errors"
	"github.com/iicpc/submission-api/internal/publisher"
	"github.com/iicpc/submission-api/internal/store"
	"github.com/iicpc/submission-api/internal/validator"
)

const maxFormBytes = validator.MaxZipBytes + 4096

// submitResponse groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type submitResponse struct {
	SubmissionID string    `json:"submission_id"`
	Status       string    `json:"status"`
	SHA256       string    `json:"sha256"`
	Language     string    `json:"language"`
	Protocol     string    `json:"protocol"`
	Port         int       `json:"port"`
	TeamName     string    `json:"team_name"`
	CreatedAt    time.Time `json:"created_at"`
}

// errorResponse groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type errorResponse struct {
	Error        string `json:"error"`
	SubmissionID string `json:"submission_id,omitempty"`
}

// Submit performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func Submit(ms *store.MinioStore, pg *store.PostgresStore, pub publisher.Publisher, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, maxFormBytes)

		if err := r.ParseMultipartForm(32 << 20); err != nil {
			metrics.Counter("submission_validation_failures_total", "Submission validation failures.", metrics.Labels("reason", "parse_form"), 1)
			writeError(w, http.StatusBadRequest, "failed to parse form: "+err.Error())
			return
		}
		defer r.MultipartForm.RemoveAll()

		file, _, err := r.FormFile("file")
		if err != nil {
			metrics.Counter("submission_validation_failures_total", "Submission validation failures.", metrics.Labels("reason", "missing_file"), 1)
			writeError(w, http.StatusBadRequest, "missing file field")
			return
		}
		defer file.Close()

		size, err := file.Seek(0, io.SeekEnd)
		if err != nil {
			log.ErrorContext(r.Context(), "failed to seek end of upload file", "error", err)
			writeError(w, http.StatusInternalServerError, "failed to read file")
			return
		}
		if _, err := file.Seek(0, io.SeekStart); err != nil {
			log.ErrorContext(r.Context(), "failed to seek start of upload file", "error", err)
			writeError(w, http.StatusInternalServerError, "failed to read file")
			return
		}

		cfg, err := validator.ValidateSubmissionZip(file, size)
		if err != nil {
			metrics.Counter("submission_validation_failures_total", "Submission validation failures.", metrics.Labels("reason", "invalid_zip"), 1)
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}

		id, err := uuid.NewV7()
		if err != nil {
			writeError(w, http.StatusInternalServerError, "failed to generate id")
			return
		}
		submissionID := id.String()
		createdAt := time.Now().UTC()
		contestantID := contestantIDFromContext(r.Context())
		if contestantID == "" {
			writeError(w, http.StatusUnauthorized, "submission requires an authenticated contestant")
			return
		}

		if _, err := file.Seek(0, io.SeekStart); err != nil {
			log.ErrorContext(r.Context(), "failed to reset upload file pointer", "error", err)
			writeError(w, http.StatusInternalServerError, "failed to read file")
			return
		}
		hasher := sha256.New()
		if _, err := io.Copy(hasher, file); err != nil {
			log.ErrorContext(r.Context(), "failed to hash upload file", "error", err)
			writeError(w, http.StatusInternalServerError, "failed to read file")
			return
		}
		sha256hex := hex.EncodeToString(hasher.Sum(nil))

		if existingID, found, err := pg.FindBySHA256(r.Context(), sha256hex); err != nil {
			log.ErrorContext(r.Context(), "sha256 lookup failed before upload", "error", err)
			writeError(w, http.StatusInternalServerError, "lookup failed")
			return
		} else if found {
			existing, err := pg.GetByID(r.Context(), existingID)
			if err != nil {
				log.ErrorContext(r.Context(), "duplicate owner lookup failed", "submission_id", existingID, "error", err)
				writeError(w, http.StatusInternalServerError, "lookup failed")
				return
			}
			// Duplicate by sha256: the identical artifact was already uploaded and
			// (usually) already built. Instead of rejecting (was: 409), accept
			// idempotently and return the existing submission so the caller can
			// re-run it — the build phase is skipped because the existing image is
			// reused. This makes re-running the same contestant cheap (no rebuild).
			if existing != nil {
				if bound, berr := claimOrResolveOwner(r.Context(), pg, existing, contestantID); berr == nil && bound != nil {
					existing = bound
				}
				metrics.Counter("submission_duplicate_total", "Duplicate submissions detected by sha256.", nil, 1)
				log.InfoContext(r.Context(), "duplicate submission accepted; reusing existing build (skip build)",
					"submission_id", existing.SubmissionID, "status", existing.Status)
				writeJSON(w, http.StatusOK, submitResponse{
					SubmissionID: existing.SubmissionID,
					Status:       existing.Status,
					SHA256:       existing.SHA256,
					Language:     existing.Language,
					Protocol:     existing.Protocol,
					Port:         existing.Port,
					TeamName:     existing.TeamName,
					CreatedAt:    existing.CreatedAt,
				})
				return
			}
			metrics.Counter("submission_duplicate_total", "Duplicate submissions detected by sha256.", nil, 1)
			writeErrorWithID(w, http.StatusConflict, "duplicate submission", existingID)
			return
		}

		uploadStart := time.Now()
		if _, err := file.Seek(0, io.SeekStart); err != nil {
			log.ErrorContext(r.Context(), "failed to reset upload file pointer", "error", err)
			writeError(w, http.StatusInternalServerError, "failed to read file")
			return
		}
		artifactPath, err := ms.Upload(r.Context(), submissionID, file, size, sha256hex)
		metrics.Histogram("minio_operation_duration_seconds", "MinIO operation duration in seconds.", metrics.Labels("operation", "artifact_upload"), metrics.SinceSeconds(uploadStart))
		if err != nil {
			log.ErrorContext(r.Context(), "minio upload failed", "submission_id", submissionID, "error", err)
			writeError(w, http.StatusInternalServerError, "failed to store artifact")
			return
		}

		meta := store.SubmissionMeta{
			SubmissionID: submissionID,
			ContestantID: contestantID,
			SHA256:       sha256hex,
			Language:     cfg.Language,
			Protocol:     cfg.Protocol,
			Port:         cfg.Port,
			TeamName:     cfg.TeamName,
			ArtifactPath: artifactPath,
			Status:       "uploaded",
			CreatedAt:    createdAt,
		}
		if err := pg.Insert(r.Context(), meta); err != nil {
			if errors.Is(err, cerrs.ErrDuplicateSubmission) {
				metrics.Counter("submission_duplicate_total", "Duplicate submissions detected by sha256.", nil, 1)
				log.ErrorContext(r.Context(), "orphaned minio artifact after duplicate insert race", "submission_id", submissionID, "artifact_path", artifactPath)
				existingID, found, findErr := pg.FindBySHA256(r.Context(), sha256hex)
				if findErr != nil {
					log.ErrorContext(r.Context(), "sha256 lookup failed during duplicate resolution", "error", findErr)
				}
				if found {
					existing, getErr := pg.GetByID(r.Context(), existingID)
					if getErr != nil {
						log.ErrorContext(r.Context(), "duplicate owner lookup failed during race resolution", "submission_id", existingID, "error", getErr)
					}
					if existing != nil {
						existing, getErr = claimOrResolveOwner(r.Context(), pg, existing, contestantID)
						if getErr != nil {
							log.ErrorContext(r.Context(), "duplicate ownership resolution failed during race resolution", "submission_id", existingID, "error", getErr)
							writeError(w, http.StatusConflict, "duplicate submission")
							return
						}
						if existing.ContestantID != contestantID {
							writeError(w, http.StatusConflict, "duplicate submission")
							return
						}
					}
					writeErrorWithID(w, http.StatusConflict, "duplicate submission", existingID)
					return
				}
				writeError(w, http.StatusConflict, "duplicate submission")
				return
			}
			log.ErrorContext(r.Context(), "postgres insert failed", "submission_id", submissionID, "error", err)
			log.ErrorContext(r.Context(), "orphaned minio artifact after postgres insert failure", "submission_id", submissionID, "artifact_path", artifactPath)
			writeError(w, http.StatusInternalServerError, "failed to save metadata")
			return
		}

		if err := pub.PublishBuildRequested(r.Context(), publisher.PublishMeta{
			SubmissionID: submissionID,
			ContestantID: meta.ContestantID,
			SHA256:       sha256hex,
			Language:     cfg.Language,
			Protocol:     cfg.Protocol,
			Port:         cfg.Port,
			BuildType:    cfg.Build.Type,
			BuildTarget:  cfg.Build.Target,
			TeamName:     cfg.TeamName,
			ArtifactPath: artifactPath,
			RequestedAt:  createdAt,
		}); err != nil {
			log.WarnContext(r.Context(), "kafka publish failed", "submission_id", submissionID, "error", err)
		}

		log.InfoContext(r.Context(), "submission accepted",
			"submission_id", submissionID,
			"language", cfg.Language,
			"protocol", cfg.Protocol,
			"port", cfg.Port,
			"team_name", cfg.TeamName,
		)
		metrics.Counter("submissions_accepted_total", "Accepted submissions by language and protocol.", metrics.Labels("language", cfg.Language, "protocol", cfg.Protocol), 1)
		metrics.Histogram("submission_upload_bytes", "Submission upload size in bytes.", nil, float64(size))

		writeJSON(w, http.StatusCreated, submitResponse{
			SubmissionID: submissionID,
			Status:       "uploaded",
			SHA256:       sha256hex,
			Language:     cfg.Language,
			Protocol:     cfg.Protocol,
			Port:         cfg.Port,
			TeamName:     cfg.TeamName,
			CreatedAt:    createdAt,
		})
	}
}

// writeJSON performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

// writeError performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, errorResponse{Error: msg})
}

// writeErrorWithID performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func writeErrorWithID(w http.ResponseWriter, status int, msg, id string) {
	writeJSON(w, status, errorResponse{Error: msg, SubmissionID: id})
}
