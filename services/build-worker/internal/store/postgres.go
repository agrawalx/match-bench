package store

import (
	"context"
	"fmt"
	"time"

	"github.com/iicpc/libs/metrics"
	"github.com/iicpc/schemas/topics"
	"github.com/jackc/pgx/v5/pgxpool"
)

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
