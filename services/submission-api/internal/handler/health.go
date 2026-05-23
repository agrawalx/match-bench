package handler

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"os"

	cerrs "github.com/iicpc/submission-api/internal/errors"
)

func Health(log *slog.Logger) http.HandlerFunc {
	resp, err := json.Marshal(map[string]string{
		"status":  "ok",
		"service": "iicpc",
	})

	if err != nil {
		log.Error("fatal: failed to marshal health response", "error", err)
		os.Exit(1)
	}

	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, writeErr := w.Write(resp)
		if writeErr != nil {
			log.ErrorContext(r.Context(), "failed to write health response", "error", writeErr)
			http.Error(w, cerrs.ErrInternal.Error(), http.StatusInternalServerError)
		}
	}
}
