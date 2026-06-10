// Package handler implements health behavior.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package handler

import (
	"encoding/json"
	"net/http"
	"sync/atomic"

	"github.com/iicpc/bot-fleet-controller/internal/controller"
)

// ReadyState groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type ReadyState struct {
	ready atomic.Bool
}

// NewReadyState performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func NewReadyState() *ReadyState { return &ReadyState{} }

// MarkReady applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (r *ReadyState) MarkReady() { r.ready.Store(true) }

// Healthz performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func Healthz(sessions *controller.SessionManager) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{
			"status":   "ok",
			"sessions": len(sessions.Snapshot()),
		})
	}
}

// Readyz performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func Readyz(r *ReadyState) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		if !r.ready.Load() {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "starting"})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
	}
}

// writeJSON performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
