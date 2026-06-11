// Package store implements postgres behavior.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
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

// SubmissionInfo groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type SubmissionInfo struct {
	SubmissionID string
	ContestantID string
	Protocol     string
	Port         int
	ImageRef     string
}

// Store groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type Store struct {
	pool *pgxpool.Pool
}

// New performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
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

// Close applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (s *Store) Close() { s.pool.Close() }

// GetSubmission applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (s *Store) GetSubmission(ctx context.Context, submissionID string) (*SubmissionInfo, error) {
	start := time.Now()
	row := s.pool.QueryRow(ctx,
		`SELECT submission_id, contestant_id, protocol, port, image_ref
		   FROM submissions WHERE submission_id = $1`,
		submissionID,
	)
	var info SubmissionInfo
	if err := row.Scan(&info.SubmissionID, &info.ContestantID, &info.Protocol, &info.Port, &info.ImageRef); err != nil {
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

// InFlightRun groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type InFlightRun struct {
	SessionID    string
	SubmissionID string
	RunGroupID   string // empty for legacy single-session runs (run_group_id IS NULL)
	Status       string
}

// ListInFlightRuns applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
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

// MarkRunFailed applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
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

// MarkRunGroupFailed applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
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

// RunStatus applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
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

// LoadScenario applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
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

var ErrScenarioNotFound = errors.New("scenario not found")

// Healthcheck applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (s *Store) Healthcheck(ctx context.Context) error {
	start := time.Now()
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	_, err := s.pool.Exec(ctx, "SELECT 1")
	recordDB("healthcheck", start, err)
	return err
}

// RecordPoolStats applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
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

// recordDB performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func recordDB(operation string, start time.Time, err error) {
	labels := metrics.Labels("service", "bot-fleet-controller", "operation", operation)
	metrics.Histogram("db_query_duration_seconds", "PostgreSQL query duration in seconds.", labels, metrics.SinceSeconds(start))
	result := "ok"
	if err != nil {
		result = "error"
	}
	metrics.Counter("db_query_total", "PostgreSQL queries by operation and result.", metrics.Labels("service", "bot-fleet-controller", "operation", operation, "result", result), 1)
}
