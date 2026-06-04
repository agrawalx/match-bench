package metrics

import (
	"net/http"
	"strconv"
	"time"
)

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) Write(p []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	return r.ResponseWriter.Write(p)
}

func (r *statusRecorder) WriteHeader(status int) {
	if r.status != 0 {
		return
	}
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

func (r *statusRecorder) Unwrap() http.ResponseWriter {
	return r.ResponseWriter
}

func (r *statusRecorder) Flush() {
	if flusher, ok := r.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

// HTTPMiddleware records RED metrics for HTTP services.
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
