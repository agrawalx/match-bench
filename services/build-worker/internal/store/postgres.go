package store

import (
	"context"
	"fmt"

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
	_, err := s.pool.Exec(ctx,
		`UPDATE submissions SET status = $1 WHERE submission_id = $2`,
		status, submissionID,
	)
	if err != nil {
		return fmt.Errorf("update status: %w", err)
	}
	return nil
}
