package handler

import (
	"net/http"
	"sync/atomic"
)

// Health endpoint contract for every HTTP service in this repo:
//   - /healthz returns 200 unconditionally (process is up).
//   - /readyz  returns 200 only after every external client this service
//     depends on has initialised. For the orchestrator that means the k8s
//     client is connected AND the in-memory slot map has been rebuilt
//     from a Pod list in the sandbox namespace.
//
// Two endpoints, not one, so a Service that is briefly unhealthy at
// startup (slot map not rebuilt yet) is not routed to by kube-proxy
// while still being kept alive by the kubelet's livenessProbe.

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
