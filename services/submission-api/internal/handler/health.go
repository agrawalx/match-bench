// Package handler implements health behavior.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
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

// Readiness performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
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

// Health performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func Health(log *slog.Logger) (http.HandlerFunc, error) {
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
