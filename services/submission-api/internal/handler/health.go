package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"
)

const healthServiceName = "submission-api"

// Readiness reports whether the service can serve traffic. Unlike /health
// (liveness — a static 200), it checks the datastore via ping so a replica whose
// Postgres connection died post-startup returns 503 and is pulled from the Service
// endpoints instead of serving errors for ~half of requests.
func Readiness(ping func(context.Context) error, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := ping(ctx); err != nil {
			log.WarnContext(r.Context(), "readiness check failed", "error", err)
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "unavailable", "service": healthServiceName})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ready", "service": healthServiceName})
	}
}

func Health(log *slog.Logger) (http.HandlerFunc, error) {
	// Pre-marshal the tiny static response once; health checks can be among the
	// hottest endpoints in production and do not need per-request allocation.
	resp, err := json.Marshal(map[string]string{
		"status":  "ok",
		"service": healthServiceName,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal health response: %w", err)
	}

	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, writeErr := w.Write(resp)
		if writeErr != nil {
			log.ErrorContext(r.Context(), "failed to write health response", "error", writeErr)
		}
	}, nil
}
