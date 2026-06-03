package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/iicpc/libs/metrics"
	"github.com/iicpc/schemas/topics"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// SubmissionInfo holds the fields the controller needs from a submission row
// to assemble a WorkloadSpec: protocol, port, and the harbor image ref.
// The controller does not own the submissions table — submission-api does.
// We only read.
type SubmissionInfo struct {
	SubmissionID string
	ContestantID string
	Protocol     string
	Port         int
}

type Store struct {
	pool *pgxpool.Pool
}

func New(ctx context.Context, dsn string) (*Store, error) {
	start := time.Now()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		recordDB("connect", start, err)
		return nil, fmt.Errorf("pgx pool: %w", err)
	}
	recordDB("connect", start, nil)
	return &Store{pool: pool}, nil
}

func (s *Store) Close() { s.pool.Close() }

// GetSubmission fetches the fields the controller needs. Returns (nil, nil)
// when the submission row does not exist.
func (s *Store) GetSubmission(ctx context.Context, submissionID string) (*SubmissionInfo, error) {
	start := time.Now()
	row := s.pool.QueryRow(ctx,
		`SELECT submission_id, contestant_id, protocol, port
		   FROM submissions WHERE submission_id = $1`,
		submissionID,
	)
	var info SubmissionInfo
	if err := row.Scan(&info.SubmissionID, &info.ContestantID, &info.Protocol, &info.Port); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			recordDB("get_submission", start, nil)
			return nil, nil
		}
		recordDB("get_submission", start, err)
		return nil, fmt.Errorf("get submission: %w", err)
	}
	recordDB("get_submission", start, nil)
	return &info, nil
}

// InFlightRun represents a runs row found in a non-terminal state on
// controller startup. Used only by the crash-recovery path.
type InFlightRun struct {
	SessionID    string
	SubmissionID string
	RunGroupID   string // empty for legacy single-session runs (run_group_id IS NULL)
	Status       string
}

// ListInFlightRuns returns every runs row in a non-terminal state.
//
// The controller is single-replica and architecturally locked at 1; on a
// restart there can be runs frozen mid-flight (the per-session goroutine
// died with the process). v1 crash recovery is "mark-failed-on-restart":
// every row returned by this query gets MarkRunFailed'd before consumers
// start. No resume logic. The user re-triggers via the frontend.
func (s *Store) ListInFlightRuns(ctx context.Context) ([]InFlightRun, error) {
	start := time.Now()
	rows, err := s.pool.Query(ctx,
		`SELECT session_id, submission_id, COALESCE(run_group_id, ''), status
		   FROM runs
		  WHERE status NOT IN ($1, $2)`,
		topics.RunStatusCompleted, topics.RunStatusFailed,
	)
	if err != nil {
		recordDB("list_in_flight_runs", start, err)
		return nil, fmt.Errorf("list in-flight runs: %w", err)
	}
	defer rows.Close()

	var out []InFlightRun
	for rows.Next() {
		var r InFlightRun
		if err := rows.Scan(&r.SessionID, &r.SubmissionID, &r.RunGroupID, &r.Status); err != nil {
			return nil, fmt.Errorf("scan in-flight run: %w", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		recordDB("list_in_flight_runs", start, err)
		return out, err
	}
	recordDB("list_in_flight_runs", start, nil)
	return out, nil
}

// MarkRunFailed is the controller's only direct write to the runs table.
//
// HARD INVARIANT for the rest of the platform: only the bot-fleet-controller
// may write the terminal statuses 'completed' or 'failed' to runs.status.
// Every other status change flows through a benchmark.status.updated Kafka
// message produced by this service and consumed by submission-api.
//
// This function is the single, explicit exception — used only by the
// startup recovery path. The exception exists because recovery must
// complete BEFORE the controller starts consuming benchmark.requested:
// otherwise a new click for a submission whose previous run is still
// in-flight will see "active run exists" (partial unique index on
// runs.submission_id WHERE status NOT IN ('completed','failed')) and be
// returned the dead session_id of the abandoned run.
//
// Do not add other callers. If you think you need one, the answer is
// almost always "produce a benchmark.status.updated message" instead.
func (s *Store) MarkRunFailed(ctx context.Context, sessionID, message string) error {
	start := time.Now()
	tag, err := s.pool.Exec(ctx,
		`UPDATE runs
		    SET status = $2, message = $3, updated_at = now()
		  WHERE session_id = $1
		    AND status NOT IN ('completed', 'failed')`,
		sessionID, topics.RunStatusFailed, message,
	)
	if err != nil {
		recordDB("mark_run_failed", start, err)
		return fmt.Errorf("mark run failed: %w", err)
	}
	_ = tag
	recordDB("mark_run_failed", start, nil)
	return nil
}

// MarkRunGroupFailed directly marks a run_group terminal-failed. This is the
// run_groups counterpart of MarkRunFailed and the SAME recovery-only exception:
// the "one active benchmark per submission" unique index lives on RUN_GROUPS
// (idx_run_groups_one_active_per_submission), not runs, so marking child runs
// failed does NOT release it — the group row must be set terminal too, or a
// re-trigger after a controller crash is permanently rejected. Called only by
// the startup recovery sweep, before benchmark.requested consumption begins.
func (s *Store) MarkRunGroupFailed(ctx context.Context, runGroupID string) error {
	if runGroupID == "" {
		return nil // legacy single-session run with no parent group
	}
	if _, err := s.pool.Exec(ctx,
		`UPDATE run_groups
		    SET status = $2, updated_at = now()
		  WHERE run_group_id = $1
		    AND status NOT IN ('completed', 'failed')`,
		runGroupID, topics.RunStatusFailed,
	); err != nil {
		return fmt.Errorf("mark run-group failed: %w", err)
	}
	return nil
}

// RunStatus returns the current status of a run, or ("", nil) if no such run.
// Used by the benchmark.requested consumer to short-circuit a redelivered
// message for a session that already reached a terminal state (crash between
// the final status publish and the Kafka offset commit re-delivers it).
func (s *Store) RunStatus(ctx context.Context, sessionID string) (string, error) {
	var status string
	err := s.pool.QueryRow(ctx,
		`SELECT status FROM runs WHERE session_id = $1`, sessionID).Scan(&status)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("query run status: %w", err)
	}
	return status, nil
}

// LoadScenario reads one row of the scenarios table. The controller calls
// this on every benchmark.requested message to materialise the TaskSpec list
// the bot-fleet workers will execute. The table is owned (created + seeded)
// by submission-api; the controller is a read-only consumer.
//
// Returns (nil, ErrScenarioNotFound) when the scenario_id does not exist —
// the controller treats this as a fatal session error (the user got a
// stale benchmark.requested for a deleted scenario row).
func (s *Store) LoadScenario(ctx context.Context, scenarioID string) (*topics.Scenario, error) {
	start := time.Now()
	row := s.pool.QueryRow(ctx,
		`SELECT scenario_id, name, duration_ns, task_specs
		   FROM scenarios WHERE scenario_id = $1`,
		scenarioID,
	)
	var sc topics.Scenario
	var taskSpecsJSON []byte
	if err := row.Scan(&sc.ScenarioID, &sc.Name, &sc.DurationNs, &taskSpecsJSON); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			recordDB("load_scenario", start, err)
			return nil, ErrScenarioNotFound
		}
		recordDB("load_scenario", start, err)
		return nil, fmt.Errorf("load scenario %q: %w", scenarioID, err)
	}
	if err := json.Unmarshal(taskSpecsJSON, &sc.TaskSpecs); err != nil {
		recordDB("load_scenario", start, err)
		return nil, fmt.Errorf("unmarshal task_specs for scenario %q: %w", scenarioID, err)
	}
	recordDB("load_scenario", start, nil)
	return &sc, nil
}

// ErrScenarioNotFound signals a missing scenario row — controller fails the
// session in that case (the trigger referenced a scenario that no longer
// exists, almost certainly because a judge deleted it after the row was
// referenced in a benchmark.requested message).
var ErrScenarioNotFound = errors.New("scenario not found")

// Healthcheck verifies the pool can issue a basic query. Used by /readyz.
func (s *Store) Healthcheck(ctx context.Context) error {
	start := time.Now()
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	_, err := s.pool.Exec(ctx, "SELECT 1")
	recordDB("healthcheck", start, err)
	return err
}

func (s *Store) RecordPoolStats() {
	stats := s.pool.Stat()
	labels := metrics.Labels("service", "bot-fleet-controller")
	metrics.Gauge("pgxpool_acquired_conns", "Acquired pgxpool connections.", labels, float64(stats.AcquiredConns()))
	metrics.Gauge("pgxpool_idle_conns", "Idle pgxpool connections.", labels, float64(stats.IdleConns()))
	metrics.Gauge("pgxpool_total_conns", "Total pgxpool connections.", labels, float64(stats.TotalConns()))
	metrics.Gauge("pgxpool_max_conns", "Maximum pgxpool connections.", labels, float64(stats.MaxConns()))
	metrics.Gauge("pgxpool_empty_acquire_total", "pgxpool empty acquire count.", labels, float64(stats.EmptyAcquireCount()))
	metrics.Gauge("pgxpool_canceled_acquire_total", "pgxpool canceled acquire count.", labels, float64(stats.CanceledAcquireCount()))
}

// recordDB makes controller DB reads/recovery writes visible to Prometheus.
//
// Problem: controller readiness depends on DB recovery and scenario lookups,
// but those paths only logged failures. Fix: emit duration/result metrics for
// every controller store operation and export pgxpool saturation.
func recordDB(operation string, start time.Time, err error) {
	labels := metrics.Labels("service", "bot-fleet-controller", "operation", operation)
	metrics.Histogram("db_query_duration_seconds", "PostgreSQL query duration in seconds.", labels, metrics.SinceSeconds(start))
	result := "ok"
	if err != nil {
		result = "error"
	}
	metrics.Counter("db_query_total", "PostgreSQL queries by operation and result.", metrics.Labels("service", "bot-fleet-controller", "operation", operation, "result", result), 1)
}
