package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/iicpc/schemas/topics"
	cerrs "github.com/iicpc/submission-api/internal/errors"
	"github.com/iicpc/submission-api/internal/store"
	"github.com/iicpc/submission-api/internal/utils"
	kafka "github.com/segmentio/kafka-go"
)

const (
	readerMinBytes = 1
	readerMaxBytes = 1 << 20
	readerMaxWait  = 250 * time.Millisecond
	commitTimeout  = 5 * time.Second
)

// BenchmarkStatusConsumer keeps the runs table in PostgreSQL in sync with
// state transitions published by the bot-fleet-controller.
//
// HARD INVARIANT: this consumer is the ONLY writer of the terminal run
// statuses 'completed' and 'failed' anywhere in the platform during normal
// operation. The single documented exception platform-wide is the
// bot-fleet-controller's startup recovery, which writes 'failed' directly
// to release the partial unique index on runs(submission_id) before its
// consumers start.
//
// The one-writer rule exists because flipping status to a terminal value
// frees the partial unique index, which lets the user re-trigger the
// benchmark. If anyone other than the controller flips it (e.g. a stuck-
// run cleanup job, an operator "force fail" endpoint), the user's retry
// can race a still-live algo pod / bot workload / orchestrator slot.
type BenchmarkStatusConsumer struct {
	reader *kafka.Reader
	pg     *store.PostgresStore
	log    *slog.Logger
}

func NewBenchmarkStatusConsumer(brokers, groupID string, pg *store.PostgresStore, log *slog.Logger) *BenchmarkStatusConsumer {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     utils.ParseBrokers(brokers),
		GroupID:     groupID,
		Topic:       topics.TopicBenchmarkStatusUpdated,
		StartOffset: kafka.FirstOffset,
		MinBytes:    readerMinBytes,
		MaxBytes:    readerMaxBytes,
		MaxWait:     readerMaxWait,
	})
	return &BenchmarkStatusConsumer{reader: r, pg: pg, log: log}
}

// Start blocks and consumes messages until ctx is cancelled.
// On decode failures, the message is committed (poison messages don't block
// the consumer). On store failures, the message is NOT committed so the next
// poll will retry.
func (c *BenchmarkStatusConsumer) Start(ctx context.Context) {
	c.log.Info("benchmark status consumer started", "topic", topics.TopicBenchmarkStatusUpdated)
	for {
		m, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			c.log.Error("fetch message failed", "error", err)
			continue
		}

		c.handleMessage(ctx, m)
	}
}

func (c *BenchmarkStatusConsumer) handleMessage(ctx context.Context, m kafka.Message) {
	var msg topics.BenchmarkStatusUpdated
	if err := json.Unmarshal(m.Value, &msg); err != nil {
		c.log.Error("unmarshal benchmark status update", "error", err, "key", string(m.Key))
		c.commitMessage("invalid benchmark status update", m, slog.String("key", string(m.Key)))
		return
	}

	if !validRunStatus(msg.Status) {
		c.log.Error("invalid benchmark run status", "session_id", msg.SessionID, "status", msg.Status)
		c.commitMessage("invalid benchmark run status", m, slog.String("session_id", msg.SessionID), slog.String("status", msg.Status))
		return
	}

	if err := c.pg.UpdateRunStatus(ctx, msg.SessionID, msg.Status, msg.Message); err != nil {
		if errors.Is(err, cerrs.ErrRunNotFound) {
			// Controller is ahead of us with a status for a run we never
			// inserted. Should not happen — log loudly and commit so we
			// don't get stuck on it.
			c.log.Warn("status update for unknown run", "session_id", msg.SessionID, "status", msg.Status)
			c.commitMessage("unknown benchmark run", m, slog.String("session_id", msg.SessionID), slog.String("status", msg.Status))
			return
		}
		c.log.Error("update run status failed", "session_id", msg.SessionID, "error", err)
		// Don't commit; retry on next poll.
		return
	}

	c.commitMessage("processed benchmark status update", m, slog.String("session_id", msg.SessionID), slog.String("status", msg.Status))
}

func (c *BenchmarkStatusConsumer) commitMessage(reason string, m kafka.Message, attrs ...slog.Attr) {
	commitCtx, cancel := context.WithTimeout(context.Background(), commitTimeout)
	defer cancel()

	if err := c.reader.CommitMessages(commitCtx, m); err != nil {
		logAttrs := []slog.Attr{
			slog.String("reason", reason),
			slog.String("topic", m.Topic),
			slog.Int("partition", m.Partition),
			slog.Int64("offset", m.Offset),
			slog.Any("error", err),
		}
		logAttrs = append(logAttrs, attrs...)
		c.log.LogAttrs(context.Background(), slog.LevelWarn, "commit failed", logAttrs...)
	}
}

func (c *BenchmarkStatusConsumer) Close() error {
	if err := c.reader.Close(); err != nil {
		c.log.Warn("benchmark status consumer close failed", "error", err)
		return err
	}
	return nil
}

func validRunStatus(status string) bool {
	switch status {
	case topics.RunStatusRequested,
		topics.RunStatusDeploying,
		topics.RunStatusWaitingReady,
		topics.RunStatusBarrierFired,
		topics.RunStatusRunning,
		topics.RunStatusCompleted,
		topics.RunStatusFailed:
		return true
	default:
		return false
	}
}
