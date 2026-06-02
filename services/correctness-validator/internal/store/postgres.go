// Package store persists the validator's output: a per-session correctness
// summary (the idempotency marker) and the full violation log for judge review.
// Mirrors the platform's pattern: createTableSQL Exec'd on startup, no migrations.
package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/iicpc/correctness-validator/internal/validate"
	"github.com/iicpc/schemas/topics"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const createTableSQL = `
CREATE TABLE IF NOT EXISTS correctness_summary (
    session_id        TEXT PRIMARY KEY,
    contestant_id     TEXT NOT NULL DEFAULT '',
    valid_fills       BIGINT NOT NULL,
    total_fills       BIGINT NOT NULL,
    correctness_score DOUBLE PRECISION NOT NULL,
    violation_count   BIGINT NOT NULL,
    phantom_fills     BIGINT NOT NULL DEFAULT 0,
    overfills         BIGINT NOT NULL DEFAULT 0,
    price_violations  BIGINT NOT NULL DEFAULT 0,
    computed_at_ns    BIGINT NOT NULL,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS correctness_violations (
    id             BIGSERIAL PRIMARY KEY,
    session_id     TEXT NOT NULL,
    contestant_id  TEXT NOT NULL DEFAULT '',
    violation_type TEXT NOT NULL,
    order_id       TEXT NOT NULL,
    reported_qty   BIGINT,
    reported_price BIGINT,
    detail         TEXT,
    detected_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_cviol_session ON correctness_violations (session_id);
`

// Record is everything persisted for one validated session.
type Record struct {
	SessionID    string
	ContestantID string
	Report       validate.Report
	ComputedAtNS uint64
}

type Store struct {
	pool *pgxpool.Pool
}

func New(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("pgx pool: %w", err)
	}
	if _, err := pool.Exec(ctx, createTableSQL); err != nil {
		pool.Close()
		return nil, fmt.Errorf("create tables: %w", err)
	}
	return &Store{pool: pool}, nil
}

func (s *Store) Close() { s.pool.Close() }

func (s *Store) Healthcheck(ctx context.Context) error { return s.pool.Ping(ctx) }

// HasSummary reports whether a session was already validated (idempotency guard).
func (s *Store) HasSummary(ctx context.Context, sessionID string) (bool, error) {
	var exists bool
	err := s.pool.QueryRow(ctx,
		"SELECT EXISTS(SELECT 1 FROM correctness_summary WHERE session_id=$1)", sessionID).
		Scan(&exists)
	return exists, err
}

// Save atomically CLAIMS a session and writes its summary + violation log in one
// transaction. It returns inserted=true only when THIS call created the summary
// row; a concurrent or repeat call for an already-validated session gets
// inserted=false and writes nothing. This claim closes the check-then-act TOCTOU
// between the HasSummary precheck and the write, so exactly one worker publishes
// the score for a session even with VALIDATOR_CONCURRENCY > 1.
func (s *Store) Save(ctx context.Context, rec Record) (bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op after commit

	tag, err := tx.Exec(ctx, `
INSERT INTO correctness_summary
    (session_id, contestant_id, valid_fills, total_fills, correctness_score, violation_count,
     phantom_fills, overfills, price_violations, computed_at_ns)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
ON CONFLICT (session_id) DO NOTHING`,
		rec.SessionID, rec.ContestantID,
		int64(rec.Report.ValidFills), int64(rec.Report.TotalFills),
		rec.Report.CorrectnessScore(), int64(rec.Report.ViolationCount()),
		int64(rec.Report.PhantomFills), int64(rec.Report.Overfills), int64(rec.Report.PriceViolations),
		int64(rec.ComputedAtNS),
	)
	if err != nil {
		return false, fmt.Errorf("insert summary: %w", err)
	}
	if tag.RowsAffected() == 0 {
		// Another worker already claimed and persisted this session.
		return false, tx.Commit(ctx)
	}

	if len(rec.Report.Violations) > 0 {
		rows := make([][]any, 0, len(rec.Report.Violations))
		for _, v := range rec.Report.Violations {
			rows = append(rows, []any{
				rec.SessionID, rec.ContestantID, string(v.Type), v.OrderID,
				int64(v.ReportedQty), v.ReportedPrice, v.Detail,
			})
		}
		if _, err := tx.CopyFrom(ctx,
			pgx.Identifier{"correctness_violations"},
			[]string{"session_id", "contestant_id", "violation_type", "order_id", "reported_qty", "reported_price", "detail"},
			pgx.CopyFromRows(rows),
		); err != nil {
			return false, fmt.Errorf("copy violations: %w", err)
		}
	}
	return true, tx.Commit(ctx)
}

// LoadScore rebuilds the published CorrectnessScoreEvent from a stored summary.
// Used to re-publish at-least-once when a redelivery finds the summary already
// persisted (e.g. the original publish failed after the summary was committed).
func (s *Store) LoadScore(ctx context.Context, sessionID string) (topics.CorrectnessScoreEvent, bool, error) {
	var (
		ev                             topics.CorrectnessScoreEvent
		valid, total, vcount, computed int64
	)
	ev.SessionID = sessionID
	err := s.pool.QueryRow(ctx, `
SELECT contestant_id, valid_fills, total_fills, correctness_score, violation_count, computed_at_ns
  FROM correctness_summary WHERE session_id=$1`, sessionID).
		Scan(&ev.ContestantID, &valid, &total, &ev.CorrectnessScore, &vcount, &computed)
	if errors.Is(err, pgx.ErrNoRows) {
		return ev, false, nil
	}
	if err != nil {
		return ev, false, fmt.Errorf("load score: %w", err)
	}
	ev.ValidFills, ev.TotalFills, ev.ViolationCount, ev.ComputedAtNS = uint64(valid), uint64(total), uint32(vcount), uint64(computed)
	return ev, true, nil
}
