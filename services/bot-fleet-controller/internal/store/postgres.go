package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

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
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("pgx pool: %w", err)
	}
	return &Store{pool: pool}, nil
}

func (s *Store) Close() { s.pool.Close() }

// GetSubmission fetches the fields the controller needs. Returns (nil, nil)
// when the submission row does not exist.
func (s *Store) GetSubmission(ctx context.Context, submissionID string) (*SubmissionInfo, error) {
	row := s.pool.QueryRow(ctx,
		`SELECT submission_id, contestant_id, protocol, port
		   FROM submissions WHERE submission_id = $1`,
		submissionID,
	)
	var info SubmissionInfo
	if err := row.Scan(&info.SubmissionID, &info.ContestantID, &info.Protocol, &info.Port); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("get submission: %w", err)
	}
	return &info, nil
}

// InFlightRun represents a runs row found in a non-terminal state on
// controller startup. Used only by the crash-recovery path.
type InFlightRun struct {
	SessionID    string
	SubmissionID string
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
	rows, err := s.pool.Query(ctx,
		`SELECT session_id, submission_id, status
		   FROM runs
		  WHERE status NOT IN ($1, $2)`,
		topics.RunStatusCompleted, topics.RunStatusFailed,
	)
	if err != nil {
		return nil, fmt.Errorf("list in-flight runs: %w", err)
	}
	defer rows.Close()

	var out []InFlightRun
	for rows.Next() {
		var r InFlightRun
		if err := rows.Scan(&r.SessionID, &r.SubmissionID, &r.Status); err != nil {
			return nil, fmt.Errorf("scan in-flight run: %w", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
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
	tag, err := s.pool.Exec(ctx,
		`UPDATE runs
		    SET status = $2, message = $3, updated_at = now()
		  WHERE session_id = $1
		    AND status NOT IN ('completed', 'failed')`,
		sessionID, topics.RunStatusFailed, message,
	)
	if err != nil {
		return fmt.Errorf("mark run failed: %w", err)
	}
	_ = tag
	return nil
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
	row := s.pool.QueryRow(ctx,
		`SELECT scenario_id, name, duration_ns, task_specs
		   FROM scenarios WHERE scenario_id = $1`,
		scenarioID,
	)
	var sc topics.Scenario
	var taskSpecsJSON []byte
	if err := row.Scan(&sc.ScenarioID, &sc.Name, &sc.DurationNs, &taskSpecsJSON); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrScenarioNotFound
		}
		return nil, fmt.Errorf("load scenario %q: %w", scenarioID, err)
	}
	if err := json.Unmarshal(taskSpecsJSON, &sc.TaskSpecs); err != nil {
		return nil, fmt.Errorf("unmarshal task_specs for scenario %q: %w", scenarioID, err)
	}
	return &sc, nil
}

// ErrScenarioNotFound signals a missing scenario row — controller fails the
// session in that case (the trigger referenced a scenario that no longer
// exists, almost certainly because a judge deleted it after the row was
// referenced in a benchmark.requested message).
var ErrScenarioNotFound = errors.New("scenario not found")

// Healthcheck verifies the pool can issue a basic query. Used by /readyz.
func (s *Store) Healthcheck(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	_, err := s.pool.Exec(ctx, "SELECT 1")
	return err
}
