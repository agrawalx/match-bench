// Package controller implements recovery behavior.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package controller

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/iicpc/bot-fleet-controller/internal/store"
	"github.com/iicpc/libs/metrics"
	"github.com/iicpc/schemas/topics"
)

// RecoverInFlightRuns performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func RecoverInFlightRuns(ctx context.Context, st *store.Store, producer *Producer, log *slog.Logger) error {
	runs, err := st.ListInFlightRuns(ctx)
	if err != nil {
		return fmt.Errorf("list in-flight runs: %w", err)
	}
	if len(runs) == 0 {
		log.Info("startup recovery: no in-flight runs")
		return nil
	}

	metrics.Counter("recovery_inflight_runs_total", "In-flight runs recovered on controller startup.", nil, float64(len(runs)))
	log.Warn("startup recovery: marking in-flight runs failed", "count", len(runs))
	const message = "controller restart — re-trigger benchmark"

	failedGroups := make(map[string]struct{})
	for _, r := range runs {
		if err := st.MarkRunFailed(ctx, r.SessionID, message); err != nil {
			log.Error("recovery: mark run failed", "session_id", r.SessionID, "error", err)
			continue
		}

		if r.RunGroupID != "" {
			if _, done := failedGroups[r.RunGroupID]; !done {
				failedGroups[r.RunGroupID] = struct{}{}
				if err := st.MarkRunGroupFailed(ctx, r.RunGroupID); err != nil {
					log.Error("recovery: mark run-group failed", "run_group_id", r.RunGroupID, "error", err)
				}
			}
		}

		if perr := producer.PublishStatus(ctx, topics.BenchmarkStatusUpdated{
			SessionID:    r.SessionID,
			SubmissionID: r.SubmissionID,
			RunGroupID:   r.RunGroupID,
			Status:       topics.RunStatusFailed,
			Message:      message,
			UpdatedAt:    time.Now().UTC(),
		}); perr != nil {
			log.Warn("recovery: publish status failed", "session_id", r.SessionID, "error", perr)
		}

		log.Info("recovery: marked run failed", "session_id", r.SessionID, "previous_status", r.Status)
	}
	return nil
}
