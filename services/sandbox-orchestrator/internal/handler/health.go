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
)

var healthzResponse = mustMarshalStaticJSON(map[string]string{"status": "ok"})
var readyzResponse = mustMarshalStaticJSON(map[string]string{"status": "ready"})

// ReadyState groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type ReadyState struct {
	ready atomic.Bool
}

// NewReadyState performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func NewReadyState() *ReadyState {
	return &ReadyState{}
}

// MarkReady applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (r *ReadyState) MarkReady() {
	r.ready.Store(true)
}

// Healthz performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func Healthz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(healthzResponse)
}

// Readyz performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func Readyz(r *ReadyState) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		if !r.ready.Load() {
			writeError(w, http.StatusServiceUnavailable, "starting up")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(readyzResponse)
	}
}

// mustMarshalStaticJSON performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func mustMarshalStaticJSON(v any) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return data
}
