package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
	"github.com/iicpc/leaderboard-api/internal/read"
)

type Reader interface {
	Leaderboard(context.Context, read.LeaderboardQuery) (read.LeaderboardResponse, error)
	RunDetail(context.Context, string) (read.RunDetail, error)
	Chart(context.Context, string) ([]read.MetricPoint, error)
}

type Handler struct {
	reader        Reader
	prometheusURL string
}

func New(reader Reader, prometheusURL string) *Handler {
	return &Handler{reader: reader, prometheusURL: prometheusURL}
}

func (h *Handler) Leaderboard(w http.ResponseWriter, r *http.Request) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	resp, err := h.reader.Leaderboard(r.Context(), read.LeaderboardQuery{
		Limit: limit,
		Sort:  r.URL.Query().Get("sort"),
		Order: r.URL.Query().Get("order"),
	})
	writeJSON(w, resp, err)
}

func (h *Handler) RunDetail(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "run_group_id")
	if id == "" {
		http.Error(w, "missing run_group_id", http.StatusBadRequest)
		return
	}
	resp, err := h.reader.RunDetail(r.Context(), id)
	writeJSON(w, resp, err)
}

func (h *Handler) Chart(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "session_id")
	if id == "" {
		http.Error(w, "missing session_id", http.StatusBadRequest)
		return
	}
	resp, err := h.reader.Chart(r.Context(), id)
	writeJSON(w, map[string]any{"session_id": id, "points": resp}, err)
}

func (h *Handler) HealthPanel(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, map[string]any{
		"source":         "prometheus",
		"prometheus_url": h.prometheusURL,
		"panels": []string{
			"kafka_consumer_lag",
			"timescaledb_write_rate",
			"capture_rate_vs_orders_sent",
			"bot_pod_count",
		},
	}, nil)
}

func writeJSON(w http.ResponseWriter, v any, err error) {
	w.Header().Set("Content-Type", "application/json")
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}
	_ = json.NewEncoder(w).Encode(v)
}
