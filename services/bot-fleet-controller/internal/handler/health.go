package handler

import (
	"encoding/json"
	"net/http"
	"sync/atomic"

	"github.com/iicpc/bot-fleet-controller/internal/controller"
)

// ReadyState is flipped after startup recovery completes and the consumers
// have started. /readyz returns 200 only after this — see CONVENTIONS.md §9.
type ReadyState struct {
	ready atomic.Bool
}

func NewReadyState() *ReadyState { return &ReadyState{} }

func (r *ReadyState) MarkReady() { r.ready.Store(true) }

func Healthz(sessions *controller.SessionManager) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{
			"status":    "ok",
			"sessions":  len(sessions.Snapshot()),
		})
	}
}

func Readyz(r *ReadyState) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		if !r.ready.Load() {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "starting"})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
