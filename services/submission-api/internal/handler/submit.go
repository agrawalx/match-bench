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
	cerrs "github.com/iicpc/submission-api/internal/errors"
	"github.com/iicpc/submission-api/internal/publisher"
	"github.com/iicpc/submission-api/internal/store"
	"github.com/iicpc/submission-api/internal/validator"
)

// maxFormBytes caps the entire multipart body (file + form overhead).
const maxFormBytes = validator.MaxZipBytes + 4096

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

type errorResponse struct {
	Error        string `json:"error"`
	SubmissionID string `json:"submission_id,omitempty"`
}

func Submit(ms *store.MinioStore, pg *store.PostgresStore, pub publisher.Publisher, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, maxFormBytes)

		if err := r.ParseMultipartForm(32 << 20); err != nil {
			writeError(w, http.StatusBadRequest, "failed to parse form: "+err.Error())
			return
		}
		// multipart temp files are not cleaned up without
		// explicit RemoveAll; relying on GC finalization leaks disk space under load
		defer r.MultipartForm.RemoveAll()

		file, _, err := r.FormFile("file")
		if err != nil {
			writeError(w, http.StatusBadRequest, "missing file field")
			return
		}
		defer file.Close()

		// Find file size using Seek.
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

		// Validate zip structure and parse benchmark.yaml.
		cfg, err := validator.ValidateSubmissionZip(file, size)
		if err != nil {
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

		// Compute SHA-256 before uploading so the object metadata and database
		// row agree on the exact digest we intended to store.
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
			writeErrorWithID(w, http.StatusConflict, "duplicate submission", existingID)
			return
		}

		// Upload artifact to MinIO.
		if _, err := file.Seek(0, io.SeekStart); err != nil {
			log.ErrorContext(r.Context(), "failed to reset upload file pointer", "error", err)
			writeError(w, http.StatusInternalServerError, "failed to read file")
			return
		}
		artifactPath, err := ms.Upload(r.Context(), submissionID, file, size, sha256hex)
		if err != nil {
			log.ErrorContext(r.Context(), "minio upload failed", "submission_id", submissionID, "error", err)
			writeError(w, http.StatusInternalServerError, "failed to store artifact")
			return
		}

		// Persist metadata to PostgreSQL.
		meta := store.SubmissionMeta{
			SubmissionID: submissionID,
			// TODO(auth): populate from authenticated contestant context.
			ContestantID: "",
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
				log.ErrorContext(r.Context(), "orphaned minio artifact after duplicate insert race", "submission_id", submissionID, "artifact_path", artifactPath)
				existingID, found, findErr := pg.FindBySHA256(r.Context(), sha256hex)
				if findErr != nil {
					log.ErrorContext(r.Context(), "sha256 lookup failed during duplicate resolution", "error", findErr)
				}
				if found {
					writeErrorWithID(w, http.StatusConflict, "duplicate submission", existingID)
					return
				}
				writeError(w, http.StatusConflict, "duplicate submission")
				return
			}
			log.ErrorContext(r.Context(), "postgres insert failed", "submission_id", submissionID, "error", err)
			// The artifact upload already succeeded and there is no atomic
			// transaction across MinIO and Postgres. Log the object path so a
			// reconciliation job or operator can clean up the orphaned object.
			log.ErrorContext(r.Context(), "orphaned minio artifact after postgres insert failure", "submission_id", submissionID, "artifact_path", artifactPath)
			writeError(w, http.StatusInternalServerError, "failed to save metadata")
			return
		}

		// Publish to Kafka — best-effort, does not fail the request. This is
		// intentionally weaker than benchmark.requested, where Kafka delivery is
		// required because the user receives a live run_id immediately.
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

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, errorResponse{Error: msg})
}

func writeErrorWithID(w http.ResponseWriter, status int, msg, id string) {
	writeJSON(w, status, errorResponse{Error: msg, SubmissionID: id})
}
