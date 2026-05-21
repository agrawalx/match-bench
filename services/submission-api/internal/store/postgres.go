package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrDuplicateSubmission = errors.New("duplicate submission")

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
CREATE UNIQUE INDEX IF NOT EXISTS submissions_sha256_idx ON submissions(sha256);
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

	if _, err := pool.Exec(ctx, createTableSQL); err != nil {
		return nil, fmt.Errorf("create submissions table: %w", err)
	}

	return &PostgresStore{pool: pool}, nil
}

func (s *PostgresStore) Close() {
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
		return "", false, fmt.Errorf("sha256 lookup: %w", err)
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
		return nil, fmt.Errorf("get submission: %w", err)
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
			return ErrDuplicateSubmission
		}
		return fmt.Errorf("insert submission: %w", err)
	}
	return nil
}
