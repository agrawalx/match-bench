package controller

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/iicpc/bot-fleet-controller/internal/store"
	"github.com/iicpc/schemas/topics"
)

// RecoverInFlightRuns marks every in-flight run as failed on controller
// startup. See CONVENTIONS.md §9 — this is the v1 recovery strategy:
// no resume logic, the user re-triggers.
//
// We write directly to PostgreSQL here (the one documented exception to
// "submission-api is the writer" in CONVENTIONS.md §6.1) because the recovery
// MUST complete synchronously before consuming benchmark.requested: a stale
// non-terminal run blocks the partial unique index and would cause a new
// click to receive the dead session_id.
//
// We also publish benchmark.status.updated so any downstream consumers
// (future sse-gateway) observe the failure — best-effort, but the
// authoritative state is already in the runs table.
func RecoverInFlightRuns(ctx context.Context, st *store.Store, producer *Producer, log *slog.Logger) error {
	runs, err := st.ListInFlightRuns(ctx)
	if err != nil {
		return fmt.Errorf("list in-flight runs: %w", err)
	}
	if len(runs) == 0 {
		log.Info("startup recovery: no in-flight runs")
		return nil
	}

	log.Warn("startup recovery: marking in-flight runs failed", "count", len(runs))
	const message = "controller restart — re-trigger benchmark"

	for _, r := range runs {
		if err := st.MarkRunFailed(ctx, r.SessionID, message); err != nil {
			// Log but keep going — partial recovery is better than none.
			log.Error("recovery: mark run failed", "session_id", r.SessionID, "error", err)
			continue
		}

		// Best-effort downstream notification. Failures here do not block
		// recovery — the authoritative state is already in PostgreSQL.
		if perr := producer.PublishStatus(ctx, topics.BenchmarkStatusUpdated{
			SessionID:    r.SessionID,
			SubmissionID: r.SubmissionID,
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
