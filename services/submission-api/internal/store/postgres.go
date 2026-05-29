package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	cerrs "github.com/iicpc/submission-api/internal/errors"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const createTableSQL = `
CREATE TABLE IF NOT EXISTS submissions (
	submission_id  TEXT PRIMARY KEY,
	contestant_id  TEXT NOT NULL DEFAULT '',
	sha256         TEXT NOT NULL UNIQUE,
	language       TEXT NOT NULL,
	protocol       TEXT NOT NULL,
	port           INT  NOT NULL,
	team_name      TEXT NOT NULL DEFAULT '',
	artifact_path  TEXT NOT NULL,
	status         TEXT NOT NULL DEFAULT 'uploaded',
	created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS runs (
	session_id     TEXT PRIMARY KEY,
	submission_id  TEXT NOT NULL,
	contestant_id  TEXT NOT NULL DEFAULT '',
	status         TEXT NOT NULL DEFAULT 'requested',
	message        TEXT NOT NULL DEFAULT '',
	created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
	updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Partial unique index enforces "at most one active run per submission" at the
-- database level. Idempotency on POST /benchmarks/{submission_id} relies on
-- this: the INSERT path either succeeds (no active row existed) or fails with
-- 23505 unique_violation, at which point the handler re-queries and returns
-- the existing run_id with HTTP 200.
--
-- HARD INVARIANT: the terminal states 'completed' and 'failed' must only be
-- written by bot-fleet-controller, via benchmark.status.updated messages
-- consumed by submission-api. The one exception is the controller's startup
-- recovery sweep, which writes 'failed' directly to release this index
-- before its consumers start.
CREATE UNIQUE INDEX IF NOT EXISTS idx_runs_one_active_per_submission
	ON runs (submission_id)
	WHERE status NOT IN ('completed', 'failed');

CREATE INDEX IF NOT EXISTS idx_runs_submission_id ON runs (submission_id);
`

type SubmissionMeta struct {
	SubmissionID string
	ContestantID string
	SHA256       string
	Language     string
	Protocol     string
	Port         int
	TeamName     string
	ArtifactPath string
	Status       string
	CreatedAt    time.Time
}

type PostgresStore struct {
	pool *pgxpool.Pool
}

func NewPostgresStore(ctx context.Context, dsn string) (*PostgresStore, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("pgx pool: %w", err)
	}

	// Bootstrap-only DDL for greenfield local/dev environments. Production
	// schema changes must go through explicit migrations; IF NOT EXISTS will
	// not evolve existing tables or indexes.
	if _, err := pool.Exec(ctx, createTableSQL); err != nil {
		return nil, fmt.Errorf("create tables: %w", err)
	}

	return &PostgresStore{pool: pool}, nil
}

// RunMeta is the in-memory shape of one row of the runs table.
type RunMeta struct {
	SessionID    string
	SubmissionID string
	ContestantID string
	Status       string
	Message      string
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// FindActiveRun returns the active (non-terminal) run for a submission, or
// (nil, nil) when there is none. Used by the benchmark endpoint to decide
// whether to mint a new session_id or return the existing one. LIMIT 1 is safe
// because idx_runs_one_active_per_submission enforces at most one active row.
func (s *PostgresStore) FindActiveRun(ctx context.Context, submissionID string) (*RunMeta, error) {
	row := s.pool.QueryRow(ctx,
		`SELECT session_id, submission_id, contestant_id, status, message, created_at, updated_at
		   FROM runs
		  WHERE submission_id = $1
		    AND status NOT IN ('completed', 'failed')
		  LIMIT 1`,
		submissionID,
	)
	var r RunMeta
	err := row.Scan(&r.SessionID, &r.SubmissionID, &r.ContestantID, &r.Status, &r.Message, &r.CreatedAt, &r.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("%w: find active run: %v", cerrs.ErrStoreDatabaseFailed, err)
	}
	return &r, nil
}

// GetRun fetches one run by session_id. Returns (nil, nil) when not found.
func (s *PostgresStore) GetRun(ctx context.Context, sessionID string) (*RunMeta, error) {
	row := s.pool.QueryRow(ctx,
		`SELECT session_id, submission_id, contestant_id, status, message, created_at, updated_at
		   FROM runs WHERE session_id = $1`,
		sessionID,
	)
	var r RunMeta
	err := row.Scan(&r.SessionID, &r.SubmissionID, &r.ContestantID, &r.Status, &r.Message, &r.CreatedAt, &r.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("%w: get run: %v", cerrs.ErrStoreDatabaseFailed, err)
	}
	return &r, nil
}

// InsertRun creates a new run row. Returns ErrActiveRunExists when the
// partial unique index rejects the insert because another active run is
// already in flight for this submission. Callers that want idempotent UX must
// handle ErrActiveRunExists by re-querying FindActiveRun and returning the
// winning row.
func (s *PostgresStore) InsertRun(ctx context.Context, r RunMeta) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO runs
			(session_id, submission_id, contestant_id, status, message, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		r.SessionID, r.SubmissionID, r.ContestantID, r.Status, r.Message, r.CreatedAt, r.UpdatedAt,
	)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			// 23505 is the unique violation code. With our schema this can
			// fire from the primary key collision (session_id) — unreachable
			// in practice because session_id is UUID v7 — or from the partial
			// unique index. Either way, the caller falls back to returning
			// the existing run_id.
			return cerrs.ErrActiveRunExists
		}
		return fmt.Errorf("%w: insert run: %v", cerrs.ErrStoreDatabaseFailed, err)
	}
	return nil
}

// UpdateRunStatus applies a benchmark.status.updated event to the runs row.
//
// This is the ONLY path that writes terminal values ('completed' or 'failed')
// into runs.status from outside the bot-fleet-controller's own startup
// recovery. Only the bot-fleet-controller emits benchmark.status.updated
// events. Do not add other callers — every other status writer in this
// repo violates the one-writer invariant that protects the partial unique
// index from being freed by an unauthorized actor.
//
// The update is intentionally monotonic and idempotent. Kafka can replay an
// already-handled event if the process crashes after PostgreSQL commits but
// before the Kafka offset commit succeeds. Replayed older states must not
// regress a run that has already advanced or reached a terminal status.
func (s *PostgresStore) UpdateRunStatus(ctx context.Context, sessionID, status, message string) error {
	tag, err := s.pool.Exec(ctx,
		`WITH incoming(rank) AS (
			SELECT CASE $2
				WHEN 'requested' THEN 0
				WHEN 'deploying' THEN 1
				WHEN 'waiting_ready' THEN 2
				WHEN 'barrier_fired' THEN 3
				WHEN 'running' THEN 4
				WHEN 'completed' THEN 5
				WHEN 'failed' THEN 5
				ELSE -1
			END
		)
		UPDATE runs
		   SET status = $2, message = $3, updated_at = now()
		  FROM incoming
		 WHERE session_id = $1
		   AND incoming.rank >= 0
		   AND (
		       runs.status = $2
		       OR (
		           runs.status NOT IN ('completed', 'failed')
		           AND incoming.rank >= CASE runs.status
		               WHEN 'requested' THEN 0
		               WHEN 'deploying' THEN 1
		               WHEN 'waiting_ready' THEN 2
		               WHEN 'barrier_fired' THEN 3
		               WHEN 'running' THEN 4
		               WHEN 'completed' THEN 5
		               WHEN 'failed' THEN 5
		               ELSE -1
		           END
		       )
		   )`,
		sessionID, status, message,
	)
	if err != nil {
		return fmt.Errorf("%w: update run status: %v", cerrs.ErrStoreDatabaseFailed, err)
	}
	if tag.RowsAffected() == 0 {
		exists, err := s.runExists(ctx, sessionID)
		if err != nil {
			return err
		}
		if exists {
			return nil
		}
		return cerrs.ErrRunNotFound
	}
	return nil
}

func (s *PostgresStore) runExists(ctx context.Context, sessionID string) (bool, error) {
	var exists bool
	err := s.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM runs WHERE session_id = $1)`, sessionID).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("%w: check run exists: %v", cerrs.ErrStoreDatabaseFailed, err)
	}
	return exists, nil
}

func (s *PostgresStore) Close() {
	// pgxpool.Close has no error return; shutdown is best-effort drain/close.
	s.pool.Close()
}

// FindBySHA256 returns the existing submission_id if a duplicate artifact is detected.
func (s *PostgresStore) FindBySHA256(ctx context.Context, sha256hex string) (submissionID string, found bool, err error) {
	row := s.pool.QueryRow(ctx,
		`SELECT submission_id FROM submissions WHERE sha256 = $1 LIMIT 1`,
		sha256hex,
	)
	err = row.Scan(&submissionID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", false, nil
		}
		return "", false, fmt.Errorf("%w: sha256 lookup: %v", cerrs.ErrStoreDatabaseFailed, err)
	}
	return submissionID, true, nil
}

// GetByID fetches a submission by its ID. Returns (nil, nil) if not found.
func (s *PostgresStore) GetByID(ctx context.Context, submissionID string) (*SubmissionMeta, error) {
	row := s.pool.QueryRow(ctx,
		`SELECT submission_id, contestant_id, sha256, language, protocol, port,
		        team_name, artifact_path, status, created_at
		 FROM submissions WHERE submission_id = $1`,
		submissionID,
	)

	var m SubmissionMeta
	err := row.Scan(
		&m.SubmissionID, &m.ContestantID, &m.SHA256, &m.Language,
		&m.Protocol, &m.Port, &m.TeamName, &m.ArtifactPath,
		&m.Status, &m.CreatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("%w: get submission: %v", cerrs.ErrStoreDatabaseFailed, err)
	}
	return &m, nil
}

// Insert writes a new submission record.
func (s *PostgresStore) Insert(ctx context.Context, m SubmissionMeta) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO submissions
			(submission_id, contestant_id, sha256, language, protocol, port,
			 team_name, artifact_path, status, created_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
		m.SubmissionID, m.ContestantID, m.SHA256, m.Language,
		m.Protocol, m.Port, m.TeamName, m.ArtifactPath,
		m.Status, m.CreatedAt,
	)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return cerrs.ErrDuplicateSubmission
		}
		return fmt.Errorf("%w: insert submission: %v", cerrs.ErrStoreDatabaseFailed, err)
	}
	return nil
}
