package store

import (
	"context"
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
// The controller marks each of them failed on startup per CONVENTIONS.md §9.
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
// Used exclusively by the startup recovery path (CONVENTIONS.md §9 + §6.1):
// every other status change flows through benchmark.status.updated Kafka
// messages consumed by submission-api.
//
// The synchronous DB write here is intentional: recovery must complete
// before the controller starts consuming benchmark.requested, otherwise a
// new click for a submission whose previous run is still in-flight will
// see "active run exists" and be returned the dead session_id.
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

// Healthcheck verifies the pool can issue a basic query. Used by /readyz.
func (s *Store) Healthcheck(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	_, err := s.pool.Exec(ctx, "SELECT 1")
	return err
}
