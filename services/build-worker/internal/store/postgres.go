package store

import (
	"context"
	"fmt"
	"time"

	"github.com/iicpc/libs/metrics"
	"github.com/iicpc/schemas/topics"
	"github.com/jackc/pgx/v5/pgxpool"
)

// startupSchemaSQL mirrors submission-api's idempotent image_ref migration.
// The column was previously owned solely by submission-api's startup DDL,
// which made build-worker's UpdateImageRef a deploy-order trap: against a
// database whose submissions table pre-dates the column, the UPDATE failed
// and the build went sticky-'failed'. Running the same ADD COLUMN IF NOT
// EXISTS here makes either service sufficient to migrate the column.
//
// ALTER TABLE IF EXISTS: on a fresh database the submissions table does not
// exist until submission-api boots and runs its CREATE TABLE (which already
// includes image_ref) — build-worker must not crashloop on that window.
const startupSchemaSQL = `
ALTER TABLE IF EXISTS submissions ADD COLUMN IF NOT EXISTS image_ref TEXT NOT NULL DEFAULT '';
`

type PostgresStore struct {
	pool *pgxpool.Pool
}

func NewPostgresStore(ctx context.Context, dsn string) (*PostgresStore, error) {
	start := time.Now()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		recordDB("connect", start, err)
		return nil, fmt.Errorf("pgx pool: %w", err)
	}
	recordDB("connect", start, nil)

	if _, err := pool.Exec(ctx, startupSchemaSQL); err != nil {
		recordDB("startup_schema_migration", start, err)
		return nil, fmt.Errorf("startup schema migration: %w", err)
	}
	recordDB("startup_schema_migration", start, nil)
	return &PostgresStore{pool: pool}, nil
}

func (s *PostgresStore) Close() {
	s.pool.Close()
}

func (s *PostgresStore) UpdateStatus(ctx context.Context, submissionID, status, message string) error {
	start := time.Now()
	if !validSubmissionStatus(status) {
		recordDB("update_submission_status", start, fmt.Errorf("invalid status"))
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
		recordDB("update_submission_status", start, err)
		return fmt.Errorf("update status: %w", err)
	}
	recordDB("update_submission_status", start, nil)
	return nil
}

func (s *PostgresStore) UpdateImageRef(ctx context.Context, submissionID, imageRef string) error {
	start := time.Now()
	if imageRef == "" {
		err := fmt.Errorf("empty image ref")
		recordDB("update_submission_image_ref", start, err)
		return err
	}
	_, err := s.pool.Exec(ctx,
		`UPDATE submissions
		    SET image_ref = $2
		  WHERE submission_id = $1`,
		submissionID, imageRef,
	)
	if err != nil {
		recordDB("update_submission_image_ref", start, err)
		return fmt.Errorf("update image ref: %w", err)
	}
	recordDB("update_submission_image_ref", start, nil)
	return nil
}

func (s *PostgresStore) RecordPoolStats() {
	stats := s.pool.Stat()
	labels := metrics.Labels("service", "build-worker")
	metrics.Gauge("pgxpool_acquired_conns", "Acquired pgxpool connections.", labels, float64(stats.AcquiredConns()))
	metrics.Gauge("pgxpool_idle_conns", "Idle pgxpool connections.", labels, float64(stats.IdleConns()))
	metrics.Gauge("pgxpool_total_conns", "Total pgxpool connections.", labels, float64(stats.TotalConns()))
	metrics.Gauge("pgxpool_max_conns", "Maximum pgxpool connections.", labels, float64(stats.MaxConns()))
	metrics.Gauge("pgxpool_empty_acquire_total", "pgxpool empty acquire count.", labels, float64(stats.EmptyAcquireCount()))
	metrics.Gauge("pgxpool_canceled_acquire_total", "pgxpool canceled acquire count.", labels, float64(stats.CanceledAcquireCount()))
}

// recordDB makes build pipeline database pressure visible to Prometheus.
func recordDB(operation string, start time.Time, err error) {
	labels := metrics.Labels("service", "build-worker", "operation", operation)
	metrics.Histogram("db_query_duration_seconds", "PostgreSQL query duration in seconds.", labels, metrics.SinceSeconds(start))
	result := "ok"
	if err != nil {
		result = "error"
	}
	metrics.Counter("db_query_total", "PostgreSQL queries by operation and result.", metrics.Labels("service", "build-worker", "operation", operation, "result", result), 1)
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
