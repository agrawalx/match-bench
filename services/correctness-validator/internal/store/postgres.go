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

// Status values for a correctness_summary row. StatusScored marks a REAL
// validation verdict and is immutable once written. StatusTimedOut marks the
// placeholder the VALIDATION_TIMEOUT fallback writes when a session could not
// be validated within budget: zero fills, zero violations — internally
// consistent, NOT a fabricated phantom fill. A timed_out row may be overwritten
// by a later real score (never the reverse), and the trigger path re-runs the
// validation when it sees one instead of treating the session as done.
const (
	StatusScored   = "scored"
	StatusTimedOut = "timed_out"
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
    status            TEXT NOT NULL DEFAULT 'scored',
    sent_count        BIGINT NOT NULL DEFAULT 0,
    acked_count       BIGINT NOT NULL DEFAULT 0,
    matched_count     BIGINT NOT NULL DEFAULT 0,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- Migration for tables created before the status column existed. ADD COLUMN IF
-- NOT EXISTS is idempotent and safe to run on every startup; every pre-existing
-- row was written by a completed validation, so the 'scored' default is correct.
ALTER TABLE correctness_summary ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'scored';
-- Migration for tables created before the telemetry-completeness counters
-- existed (sent/acked events drained and orders matched across both streams).
-- Idempotent like the status migration. Pre-existing rows default to 0 = the
-- counters are UNKNOWN (never "measured empty"), which downstream coverage
-- gating treats as ungateable rather than incomplete.
ALTER TABLE correctness_summary ADD COLUMN IF NOT EXISTS sent_count BIGINT NOT NULL DEFAULT 0;
ALTER TABLE correctness_summary ADD COLUMN IF NOT EXISTS acked_count BIGINT NOT NULL DEFAULT 0;
ALTER TABLE correctness_summary ADD COLUMN IF NOT EXISTS matched_count BIGINT NOT NULL DEFAULT 0;
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

// Record is everything persisted for one validated session. Status is
// StatusScored or StatusTimedOut; empty is normalized to StatusScored so
// pre-status callers keep their old semantics. SentCount/AckedCount/
// MatchedCount are the telemetry-completeness counters from the drain
// (pipeline.Counts); a timed_out placeholder leaves them zero — the drain
// never completed, so the counters are unknown, not "measured empty".
type Record struct {
	SessionID    string
	ContestantID string
	Report       validate.Report
	Status       string
	SentCount    uint64
	AckedCount   uint64
	MatchedCount uint64
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

// SummaryStatus returns the persisted summary status for a session
// (StatusScored or StatusTimedOut) and whether a summary row exists at all.
// The trigger path uses it to decide between re-publishing a real score and
// RE-RUNNING the validation: a timed_out placeholder must not satisfy the
// idempotency guard, or one slow drain would permanently freeze the
// contestant's correctness at the placeholder.
func (s *Store) SummaryStatus(ctx context.Context, sessionID string) (string, bool, error) {
	var status string
	err := s.pool.QueryRow(ctx,
		"SELECT status FROM correctness_summary WHERE session_id=$1", sessionID).
		Scan(&status)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("summary status: %w", err)
	}
	return status, true, nil
}

// Save atomically CLAIMS a session and writes its summary + violation log in one
// transaction. It returns claimed=true when THIS call either created the summary
// row or replaced a timed_out placeholder with a real scored result — the two
// cases where the caller owns publishing the score. The conditional upsert is
// one-way: a real score may overwrite a timeout placeholder, but NOTHING ever
// overwrites a scored row, so a fabricated timeout can never beat a concurrent
// real validation to the permanent verdict. This claim closes the check-then-act
// TOCTOU between the SummaryStatus precheck and the write, so exactly one worker
// publishes the score for a session even with VALIDATOR_CONCURRENCY > 1.
func (s *Store) Save(ctx context.Context, rec Record) (bool, error) {
	status := rec.Status
	if status == "" {
		status = StatusScored
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op after commit

	tag, err := tx.Exec(ctx, `
INSERT INTO correctness_summary
    (session_id, contestant_id, valid_fills, total_fills, correctness_score, violation_count,
     phantom_fills, overfills, price_violations, computed_at_ns, status,
     sent_count, acked_count, matched_count)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
ON CONFLICT (session_id) DO UPDATE SET
    contestant_id     = EXCLUDED.contestant_id,
    valid_fills       = EXCLUDED.valid_fills,
    total_fills       = EXCLUDED.total_fills,
    correctness_score = EXCLUDED.correctness_score,
    violation_count   = EXCLUDED.violation_count,
    phantom_fills     = EXCLUDED.phantom_fills,
    overfills         = EXCLUDED.overfills,
    price_violations  = EXCLUDED.price_violations,
    computed_at_ns    = EXCLUDED.computed_at_ns,
    status            = EXCLUDED.status,
    sent_count        = EXCLUDED.sent_count,
    acked_count       = EXCLUDED.acked_count,
    matched_count     = EXCLUDED.matched_count
WHERE correctness_summary.status = 'timed_out' AND EXCLUDED.status = 'scored'`,
		rec.SessionID, rec.ContestantID,
		int64(rec.Report.ValidFills), int64(rec.Report.TotalFills),
		rec.Report.CorrectnessScore(), int64(rec.Report.ViolationCount()),
		int64(rec.Report.PhantomFills), int64(rec.Report.Overfills), int64(rec.Report.PriceViolations),
		int64(rec.ComputedAtNS), status,
		int64(rec.SentCount), int64(rec.AckedCount), int64(rec.MatchedCount),
	)
	if err != nil {
		return false, fmt.Errorf("insert summary: %w", err)
	}
	if tag.RowsAffected() == 0 {
		// Conflict and the guard refused the update: an immutable scored row (or
		// a same-status placeholder) already exists. Not ours to publish.
		return false, tx.Commit(ctx)
	}
	// RowsAffected()==1 covers both the fresh insert and the
	// scored-over-timed_out overwrite. A timed_out placeholder never writes
	// violations, so the violation log below cannot double up on the overwrite.

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
		sent, acked, matched           int64
	)
	ev.SessionID = sessionID
	err := s.pool.QueryRow(ctx, `
SELECT contestant_id, valid_fills, total_fills, correctness_score, violation_count, computed_at_ns,
       sent_count, acked_count, matched_count
  FROM correctness_summary WHERE session_id=$1`, sessionID).
		Scan(&ev.ContestantID, &valid, &total, &ev.CorrectnessScore, &vcount, &computed,
			&sent, &acked, &matched)
	if errors.Is(err, pgx.ErrNoRows) {
		return ev, false, nil
	}
	if err != nil {
		return ev, false, fmt.Errorf("load score: %w", err)
	}
	ev.ValidFills, ev.TotalFills, ev.ViolationCount, ev.ComputedAtNS = uint64(valid), uint64(total), uint32(vcount), uint64(computed)
	ev.SentCount, ev.AckedCount, ev.MatchedCount = uint64(sent), uint64(acked), uint64(matched)
	return ev, true, nil
}
