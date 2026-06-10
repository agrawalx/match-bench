// Package metrics defines shared library behavior for http.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package metrics

import (
	"net/http"
	"strconv"
	"time"
)

// statusRecorder groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

// Write applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (r *statusRecorder) Write(p []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	return r.ResponseWriter.Write(p)
}

// WriteHeader applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (r *statusRecorder) WriteHeader(status int) {
	if r.status != 0 {
		return
	}
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

// Unwrap applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (r *statusRecorder) Unwrap() http.ResponseWriter {
	return r.ResponseWriter
}

// Flush applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (r *statusRecorder) Flush() {
	if flusher, ok := r.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

// HTTPMiddleware performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func HTTPMiddleware(service string, route func(*http.Request) string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			rec := &statusRecorder{ResponseWriter: w}
			next.ServeHTTP(rec, r)
			status := rec.status
			if status == 0 {
				status = http.StatusOK
			}

			path := r.URL.Path
			if route != nil {
				if routed := route(r); routed != "" {
					path = routed
				}
			}
			labels := Labels(
				"service", service,
				"method", r.Method,
				"path", path,
				"status", strconv.Itoa(status),
			)
			Counter("http_requests_total", "HTTP requests by service, method, path, and status.", labels, 1)
			Histogram("http_request_duration_seconds", "HTTP request duration in seconds.", labels, SinceSeconds(start))
		})
	}
}
