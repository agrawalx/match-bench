// Package store persists the validator's output: a per-session correctness
// summary (the idempotency marker) and the full violation log for judge review.
// Mirrors the platform's pattern: createTableSQL Exec'd on startup, no migrations.
package store

import (
	"context"
	"fmt"

	"github.com/iicpc/correctness-validator/internal/validate"
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

// Save writes the violation log + summary for a session in one transaction. The
// summary is upserted last (it is the idempotency marker), and the session's
// prior violations are cleared first so a re-run is consistent.
func (s *Store) Save(ctx context.Context, rec Record) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op after commit

	if _, err := tx.Exec(ctx, "DELETE FROM correctness_violations WHERE session_id=$1", rec.SessionID); err != nil {
		return fmt.Errorf("clear violations: %w", err)
	}

	if len(rec.Report.Violations) > 0 {
		rows := make([][]any, 0, len(rec.Report.Violations))
		for _, v := range rec.Report.Violations {
			rows = append(rows, []any{
				rec.SessionID, rec.ContestantID, string(v.Type), v.OrderID,
				int64(v.ReportedQty), v.ReportedPrice, v.Detail,
			})
		}
		_, err := tx.CopyFrom(ctx,
			pgx.Identifier{"correctness_violations"},
			[]string{"session_id", "contestant_id", "violation_type", "order_id", "reported_qty", "reported_price", "detail"},
			pgx.CopyFromRows(rows),
		)
		if err != nil {
			return fmt.Errorf("copy violations: %w", err)
		}
	}

	_, err = tx.Exec(ctx, `
INSERT INTO correctness_summary
    (session_id, contestant_id, valid_fills, total_fills, correctness_score, violation_count,
     phantom_fills, overfills, price_violations, computed_at_ns)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
ON CONFLICT (session_id) DO UPDATE SET
    contestant_id=EXCLUDED.contestant_id, valid_fills=EXCLUDED.valid_fills,
    total_fills=EXCLUDED.total_fills, correctness_score=EXCLUDED.correctness_score,
    violation_count=EXCLUDED.violation_count, phantom_fills=EXCLUDED.phantom_fills,
    overfills=EXCLUDED.overfills, price_violations=EXCLUDED.price_violations,
    computed_at_ns=EXCLUDED.computed_at_ns, created_at=now()`,
		rec.SessionID, rec.ContestantID,
		int64(rec.Report.ValidFills), int64(rec.Report.TotalFills),
		rec.Report.CorrectnessScore(), int64(rec.Report.ViolationCount()),
		int64(rec.Report.PhantomFills), int64(rec.Report.Overfills), int64(rec.Report.PriceViolations),
		int64(rec.ComputedAtNS),
	)
	if err != nil {
		return fmt.Errorf("upsert summary: %w", err)
	}
	return tx.Commit(ctx)
}
