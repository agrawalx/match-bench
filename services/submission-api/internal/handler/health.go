package handler

import (
	"encoding/json"
	"net/http"
)

func Health() http.HandlerFunc {
	resp, _ := json.Marshal(map[string]string{
		"status":  "ok",
		"service": "submission-api",
	})
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(resp)
	}
}
