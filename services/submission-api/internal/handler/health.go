package handler

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
)

const healthServiceName = "submission-api"

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
