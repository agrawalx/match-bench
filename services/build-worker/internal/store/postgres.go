package store

import (
	"context"
	"fmt"

	"github.com/iicpc/schemas/topics"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresStore struct {
	pool *pgxpool.Pool
}

func NewPostgresStore(ctx context.Context, dsn string) (*PostgresStore, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("pgx pool: %w", err)
	}
	return &PostgresStore{pool: pool}, nil
}

func (s *PostgresStore) Close() {
	s.pool.Close()
}

func (s *PostgresStore) UpdateStatus(ctx context.Context, submissionID, status, message string) error {
	if !validSubmissionStatus(status) {
		return fmt.Errorf("invalid submission status: %s", status)
	}
	_, err := s.pool.Exec(ctx,
		`WITH incoming(rank) AS (
			SELECT CASE $2
				WHEN 'uploaded' THEN 0
				WHEN 'building' THEN 1
				WHEN 'scanned' THEN 2
				WHEN 'sbom_ready' THEN 2
				WHEN 'ready' THEN 3
				WHEN 'failed' THEN 4
				ELSE -1
			END
		)
		UPDATE submissions
		   SET status = $2
		  FROM incoming
		 WHERE submission_id = $1
		   AND incoming.rank >= 0
		   AND (
		       submissions.status = $2
		       OR (
		           submissions.status != 'failed'
		           AND incoming.rank >= CASE submissions.status
		               WHEN 'uploaded' THEN 0
		               WHEN 'building' THEN 1
		               WHEN 'scanned' THEN 2
		               WHEN 'sbom_ready' THEN 2
		               WHEN 'ready' THEN 3
		               WHEN 'failed' THEN 4
		               ELSE -1
		           END
		       )
		   )`,
		submissionID, status,
	)
	if err != nil {
		return fmt.Errorf("update status: %w", err)
	}
	return nil
}

func validSubmissionStatus(status string) bool {
	switch status {
	case topics.StatusUploaded,
		topics.StatusBuilding,
		topics.StatusScanned,
		topics.StatusSBOMReady,
		topics.StatusReady,
		topics.StatusFailed:
		return true
	default:
		return false
	}
}
