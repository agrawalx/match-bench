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

	metrics.Counter("recovery_inflight_runs_total", "In-flight runs recovered on controller startup.", nil, float64(len(runs)))
	log.Warn("startup recovery: marking in-flight runs failed", "count", len(runs))
	const message = "controller restart — re-trigger benchmark"

	failedGroups := make(map[string]struct{})
	for _, r := range runs {
		if err := st.MarkRunFailed(ctx, r.SessionID, message); err != nil {
			// Log but keep going — partial recovery is better than none.
			log.Error("recovery: mark run failed", "session_id", r.SessionID, "error", err)
			continue
		}

		// CRITICAL: the "one active benchmark per submission" unique index lives
		// on run_groups, NOT runs. Marking child runs failed does not release it,
		// so the parent group must be set terminal directly here — otherwise a
		// re-trigger for this submission is permanently rejected after a crash
		// (the dead group keeps occupying the index). Done once per distinct group.
		if r.RunGroupID != "" {
			if _, done := failedGroups[r.RunGroupID]; !done {
				failedGroups[r.RunGroupID] = struct{}{}
				if err := st.MarkRunGroupFailed(ctx, r.RunGroupID); err != nil {
					log.Error("recovery: mark run-group failed", "run_group_id", r.RunGroupID, "error", err)
				}
			}
		}

		// Best-effort downstream notification. RunGroupID MUST be populated so the
		// submission-api consumer runs its run-group rollup (it skips the rollup
		// when RunGroupID is empty), keeping the denormalized group status
		// consistent with what we just wrote directly.
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
