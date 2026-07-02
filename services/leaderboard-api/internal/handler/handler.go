// Package handler implements handler behavior.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
	"github.com/iicpc/leaderboard-api/internal/read"
)

// Reader defines the behavior expected by this package boundary.
// Implementations should preserve the caller-visible contract.
type Reader interface {
	Leaderboard(context.Context, read.LeaderboardQuery) (read.LeaderboardResponse, error)
	RunDetail(context.Context, string) (read.RunDetail, error)
	Chart(context.Context, string) ([]read.MetricPoint, error)
	ActiveRuns(context.Context) ([]read.ActiveRun, error)
}

// Handler groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type Handler struct {
	reader        Reader
	prometheusURL string
}

// New performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func New(reader Reader, prometheusURL string) *Handler {
	return &Handler{reader: reader, prometheusURL: prometheusURL}
}

// Leaderboard applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (h *Handler) Leaderboard(w http.ResponseWriter, r *http.Request) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	resp, err := h.reader.Leaderboard(r.Context(), read.LeaderboardQuery{
		Limit:        limit,
		Sort:         r.URL.Query().Get("sort"),
		Order:        r.URL.Query().Get("order"),
		Cursor:       r.URL.Query().Get("cursor"),
		RunGroupID:   r.URL.Query().Get("run_group_id"),
		SubmissionID: r.URL.Query().Get("submission_id"),
		ContestantID: r.URL.Query().Get("contestant_id"),
		TeamID:       r.URL.Query().Get("team_id"),
		TeamName:     r.URL.Query().Get("team_name"),
		Scenario:     r.URL.Query().Get("scenario"),
	})
	writeJSON(w, resp, err)
}

// RunDetail applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (h *Handler) RunDetail(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "run_group_id")
	if id == "" {
		http.Error(w, "missing run_group_id", http.StatusBadRequest)
		return
	}
	resp, err := h.reader.RunDetail(r.Context(), id)
	writeJSON(w, resp, err)
}

// Chart applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (h *Handler) Chart(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "session_id")
	if id == "" {
		http.Error(w, "missing session_id", http.StatusBadRequest)
		return
	}
	resp, err := h.reader.Chart(r.Context(), id)
	writeJSON(w, map[string]any{"session_id": id, "points": resp}, err)
}

// LiveRuns applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (h *Handler) LiveRuns(w http.ResponseWriter, r *http.Request) {
	runs, err := h.reader.ActiveRuns(r.Context())
	if runs == nil {
		runs = []read.ActiveRun{}
	}
	writeJSON(w, map[string]any{"runs": runs}, err)
}

// HealthPanel applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
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

// writeJSON performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func writeJSON(w http.ResponseWriter, v any, err error) {
	w.Header().Set("Content-Type", "application/json")
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}
	_ = json.NewEncoder(w).Encode(v)
}
