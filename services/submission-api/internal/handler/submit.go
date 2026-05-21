package handler

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"
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

		file, _, err := r.FormFile("file")
		if err != nil {
			writeError(w, http.StatusBadRequest, "missing file field")
			return
		}
		defer file.Close()

		// Buffer the upload and compute SHA-256 in a single pass.
		hasher := sha256.New()
		var buf bytes.Buffer
		if _, err := io.Copy(io.MultiWriter(&buf, hasher), file); err != nil {
			writeError(w, http.StatusInternalServerError, "failed to read file")
			return
		}
		data := buf.Bytes()
		sha256hex := hex.EncodeToString(hasher.Sum(nil))

		// Validate zip structure and parse benchmark.yaml.
		cfg, err := validator.ValidateSubmissionZip(data)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}

		// SHA-256 dedup check.
		existingID, found, err := pg.FindBySHA256(r.Context(), sha256hex)
		if err != nil {
			log.Error("sha256 lookup failed", "error", err)
			writeError(w, http.StatusInternalServerError, "internal error")
			return
		}
		if found {
			writeErrorWithID(w, http.StatusConflict, "duplicate submission", existingID)
			return
		}

		id, err := uuid.NewV7()
		if err != nil {
			writeError(w, http.StatusInternalServerError, "failed to generate id")
			return
		}
		submissionID := id.String()
		createdAt := time.Now().UTC()

		// Upload artifact to MinIO.
		artifactPath, err := ms.Upload(r.Context(), submissionID, bytes.NewReader(data), int64(len(data)))
		if err != nil {
			log.Error("minio upload failed", "submission_id", submissionID, "error", err)
			writeError(w, http.StatusInternalServerError, "failed to store artifact")
			return
		}

		// Persist metadata to PostgreSQL.
		meta := store.SubmissionMeta{
			SubmissionID: submissionID,
			ContestantID: "",
			SHA256:       sha256hex,
			Language:     cfg.Language,
			Protocol:     cfg.Protocol,
			Port:         cfg.DeclaredPort(),
			TeamName:     cfg.TeamName,
			ArtifactPath: artifactPath,
			Status:       "uploaded",
			CreatedAt:    createdAt,
		}
		if err := pg.Insert(r.Context(), meta); err != nil {
			log.Error("postgres insert failed", "submission_id", submissionID, "error", err)
			writeError(w, http.StatusInternalServerError, "failed to save metadata")
			return
		}

		// Publish to Kafka — best-effort, does not fail the request.
		if err := pub.PublishBuildRequested(r.Context(), publisher.PublishMeta{
			SubmissionID: submissionID,
			SHA256:       sha256hex,
			Language:     cfg.Language,
			Protocol:     cfg.Protocol,
			Port:         cfg.DeclaredPort(),
			TeamName:     cfg.TeamName,
			ArtifactPath: artifactPath,
			RequestedAt:  createdAt,
		}); err != nil {
			log.Warn("kafka publish failed", "submission_id", submissionID, "error", err)
		}

		writeJSON(w, http.StatusCreated, submitResponse{
			SubmissionID: submissionID,
			Status:       "uploaded",
			SHA256:       sha256hex,
			Language:     cfg.Language,
			Protocol:     cfg.Protocol,
			Port:         cfg.DeclaredPort(),
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
