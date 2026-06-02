package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/iicpc/libs/metrics"
	"github.com/iicpc/schemas/topics"
	cerrs "github.com/iicpc/submission-api/internal/errors"
	"github.com/iicpc/submission-api/internal/utils"
	kafka "github.com/segmentio/kafka-go"
)

type RunStatusStore interface {
	UpdateRunStatus(ctx context.Context, sessionID, status, message string) error
	RecomputeRunGroupStatus(ctx context.Context, runGroupID string) error
}

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
	pg     RunStatusStore
	log    *slog.Logger
}

func NewBenchmarkStatusConsumer(brokers, groupID string, pg RunStatusStore, log *slog.Logger) *BenchmarkStatusConsumer {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        utils.ParseBrokers(brokers),
		GroupID:        groupID,
		Topic:          topics.TopicBenchmarkStatusUpdated,
		MinBytes:       1,
		MaxBytes:       1 << 20,
		MaxWait:        100 * time.Millisecond,
		CommitInterval: 0,
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
			recordConsume(topics.TopicBenchmarkStatusUpdated, "fetch_error", 0)
			c.log.Error("fetch message failed", "error", err)
			continue
		}
		start := time.Now()

		var msg topics.BenchmarkStatusUpdated
		if err := json.Unmarshal(m.Value, &msg); err != nil {
			recordConsume(topics.TopicBenchmarkStatusUpdated, "decode_error", metrics.SinceSeconds(start))
			c.log.Error("unmarshal benchmark status update", "error", err, "key", string(m.Key))
			recordCommit(topics.TopicBenchmarkStatusUpdated, c.reader.CommitMessages(ctx, m))
			continue
		}
		if !validRunStatus(msg.Status) {
			recordConsume(topics.TopicBenchmarkStatusUpdated, "invalid_status", metrics.SinceSeconds(start))
			c.log.Error("invalid benchmark run status", "session_id", msg.SessionID, "status", msg.Status)
			recordCommit(topics.TopicBenchmarkStatusUpdated, c.reader.CommitMessages(ctx, m))
			continue
		}

		if err := c.pg.UpdateRunStatus(ctx, msg.SessionID, msg.Status, msg.Message); err != nil {
			if errors.Is(err, cerrs.ErrRunNotFound) {
				recordConsume(topics.TopicBenchmarkStatusUpdated, "unknown_run", metrics.SinceSeconds(start))
				// Controller is ahead of us with a status for a run we never
				// inserted. Should not happen — log loudly and commit so we
				// don't get stuck on it.
				c.log.Warn("status update for unknown run", "session_id", msg.SessionID, "status", msg.Status)
				recordCommit(topics.TopicBenchmarkStatusUpdated, c.reader.CommitMessages(ctx, m))
				continue
			}
			recordConsume(topics.TopicBenchmarkStatusUpdated, "store_error", metrics.SinceSeconds(start))
			c.log.Error("update run status failed", "session_id", msg.SessionID, "error", err)
			// Don't commit; retry on next poll.
			continue
		}
		metrics.Counter("run_status_updates_total", "Run status updates applied by submission-api.", metrics.Labels("status", msg.Status), 1)

		// Roll the child's new status up into the parent run-group's denormalized
		// status field. A failed rollup is logged but not fatal — the children
		// are the source of truth, so a stale group row is a UX/leaderboard
		// issue, not a correctness issue. We commit the Kafka offset regardless.
		if msg.RunGroupID != "" {
			if err := c.pg.RecomputeRunGroupStatus(ctx, msg.RunGroupID); err != nil {
				c.log.Warn("recompute run-group status failed",
					"run_group_id", msg.RunGroupID,
					"session_id", msg.SessionID,
					"error", err)
			}
		}

		recordConsume(topics.TopicBenchmarkStatusUpdated, "ok", metrics.SinceSeconds(start))
		if err := c.reader.CommitMessages(ctx, m); err != nil {
			recordCommit(topics.TopicBenchmarkStatusUpdated, err)
			c.log.Warn("commit failed", "session_id", msg.SessionID, "error", err)
		} else {
			recordCommit(topics.TopicBenchmarkStatusUpdated, nil)
		}
	}
}

func (c *BenchmarkStatusConsumer) Close() error {
	return c.reader.Close()
}

// recordConsume/recordCommit make the run-status pipeline observable.
func recordConsume(topic, result string, durationSeconds float64) {
	labels := metrics.Labels("service", "submission-api", "topic", topic, "result", result)
	metrics.Counter("kafka_messages_consumed_total", "Kafka messages consumed by topic and result.", labels, 1)
	if durationSeconds > 0 {
		metrics.Histogram("kafka_message_process_duration_seconds", "Kafka message processing duration in seconds.", labels, durationSeconds)
	}
}

func recordCommit(topic string, err error) {
	result := "ok"
	if err != nil {
		result = "error"
	}
	metrics.Counter("kafka_consumer_commit_total", "Kafka consumer commits by topic and result.", metrics.Labels("service", "submission-api", "topic", topic, "result", result), 1)
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
