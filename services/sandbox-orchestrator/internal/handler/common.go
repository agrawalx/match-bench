// Package handler implements common behavior.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package handler

import (
	"encoding/json"
	"net/http"
)

// errorResponse groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type errorResponse struct {
	Error string `json:"error"`
}

// writeJSON performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// writeError performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, errorResponse{Error: msg})
}
