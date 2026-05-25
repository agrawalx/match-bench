package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"

	"github.com/iicpc/schemas/topics"
	cerrs "github.com/iicpc/submission-api/internal/errors"
	"github.com/iicpc/submission-api/internal/store"
	kafka "github.com/segmentio/kafka-go"
)

const TopicBenchmarkStatusUpdated = "benchmark.status.updated"

// BenchmarkStatusConsumer keeps the runs table in PostgreSQL in sync with
// state transitions published by the bot-fleet-controller.
//
// This is the ONLY writer of terminal run status (completed / failed) in
// the submission-api process. See CONVENTIONS.md §6.1.
type BenchmarkStatusConsumer struct {
	reader *kafka.Reader
	pg     *store.PostgresStore
	log    *slog.Logger
}

func NewBenchmarkStatusConsumer(brokers, groupID string, pg *store.PostgresStore, log *slog.Logger) *BenchmarkStatusConsumer {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{brokers},
		GroupID: groupID,
		Topic:   TopicBenchmarkStatusUpdated,
	})
	return &BenchmarkStatusConsumer{reader: r, pg: pg, log: log}
}

// Start blocks and consumes messages until ctx is cancelled.
// On decode failures, the message is committed (poison messages don't block
// the consumer). On store failures, the message is NOT committed so the next
// poll will retry.
func (c *BenchmarkStatusConsumer) Start(ctx context.Context) {
	c.log.Info("benchmark status consumer started", "topic", TopicBenchmarkStatusUpdated)
	for {
		m, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			c.log.Error("fetch message failed", "error", err)
			continue
		}

		var msg topics.BenchmarkStatusUpdated
		if err := json.Unmarshal(m.Value, &msg); err != nil {
			c.log.Error("unmarshal benchmark status update", "error", err, "key", string(m.Key))
			_ = c.reader.CommitMessages(ctx, m)
			continue
		}

		if err := c.pg.UpdateRunStatus(ctx, msg.SessionID, msg.Status, msg.Message); err != nil {
			if errors.Is(err, cerrs.ErrRunNotFound) {
				// Controller is ahead of us with a status for a run we never
				// inserted. Should not happen — log loudly and commit so we
				// don't get stuck on it.
				c.log.Warn("status update for unknown run", "session_id", msg.SessionID, "status", msg.Status)
				_ = c.reader.CommitMessages(ctx, m)
				continue
			}
			c.log.Error("update run status failed", "session_id", msg.SessionID, "error", err)
			// Don't commit; retry on next poll.
			continue
		}

		if err := c.reader.CommitMessages(ctx, m); err != nil {
			c.log.Warn("commit failed", "session_id", msg.SessionID, "error", err)
		}
	}
}

func (c *BenchmarkStatusConsumer) Close() error {
	return c.reader.Close()
}
