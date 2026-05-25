package handler

import (
	"net/http"
	"sync/atomic"
)

// Health endpoints follow CONVENTIONS.md §4: /healthz always 200,
// /readyz 200 only after all external clients (k8s, in this case)
// have initialised and the slot map has been rebuilt from k8s.

// ReadyState is a process-wide atomic flag flipped to true once startup
// initialisation completes. main.go sets it after slot restore.
type ReadyState struct {
	ready atomic.Bool
}

func NewReadyState() *ReadyState {
	return &ReadyState{}
}

func (r *ReadyState) MarkReady() {
	r.ready.Store(true)
}

func Healthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func Readyz(r *ReadyState) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		if !r.ready.Load() {
			writeError(w, http.StatusServiceUnavailable, "starting up")
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
	}
}
