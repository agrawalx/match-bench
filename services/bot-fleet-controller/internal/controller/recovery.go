package controller

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/iicpc/bot-fleet-controller/internal/store"
	"github.com/iicpc/schemas/topics"
)

// RecoverInFlightRuns marks every in-flight run as failed on controller startup.
//
// v1 crash recovery strategy: no resume logic. Every run row in a non-terminal
// state (requested|deploying|waiting_ready|barrier_fired|running) is updated
// to 'failed' with message="controller restart — re-trigger benchmark". The
// user re-triggers via the frontend. Resume would require reconstructing the
// per-session goroutine state (slot endpoint, ready-set, barrier epoch, etc.)
// from external sources that don't have it — not worth building until v2.
//
// HARD INVARIANT under normal operation: only submission-api writes terminal
// runs.status values, via its consumer of benchmark.status.updated produced
// by this controller. This recovery path is the SINGLE EXPLICIT EXCEPTION:
// we write 'failed' directly to PostgreSQL because recovery MUST complete
// synchronously BEFORE the benchmark.requested consumer starts. Otherwise a
// stale non-terminal row blocks the partial unique index on submission_id,
// and a new click for that submission gets back the dead session_id instead
// of starting a fresh run.
//
// We also publish benchmark.status.updated as a courtesy so any downstream
// consumers (e.g. a future sse-gateway pushing live status to the frontend)
// observe the failure — best-effort, the authoritative state is already in
// PostgreSQL by the time we publish.
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
