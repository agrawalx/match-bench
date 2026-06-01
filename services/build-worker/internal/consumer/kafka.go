package consumer

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"time"

	"github.com/iicpc/schemas/topics"
	kafka "github.com/segmentio/kafka-go"
)

// Handler processes one decoded build request. Implementations must treat
// SubmissionID as the idempotency key because Kafka can redeliver messages.
type Handler interface {
	Run(ctx context.Context, msg topics.SubmissionBuildRequested)
}

type Consumer struct {
	reader  *kafka.Reader
	handler Handler
	log     *slog.Logger
}

func NewKafkaConsumer(brokers, groupID string, handler Handler, log *slog.Logger) *Consumer {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        parseBrokers(brokers),
		GroupID:        groupID,
		Topic:          topics.TopicSubmissionBuildRequested,
		MinBytes:       1,
		MaxBytes:       1 << 20,
		MaxWait:        100 * time.Millisecond,
		CommitInterval: 0,
	})
	return &Consumer{reader: r, handler: handler, log: log}
}

// Start blocks and processes messages until ctx is cancelled.
func (c *Consumer) Start(ctx context.Context) {
	c.log.Info("consumer started", "topic", topics.TopicSubmissionBuildRequested)
	for {
		m, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return // shutdown
			}
			c.log.Error("fetch message failed", "error", err)
			continue
		}

		var msg topics.SubmissionBuildRequested
		if err := json.Unmarshal(m.Value, &msg); err != nil {
			c.log.Error("unmarshal failed", "error", err)
			_ = c.reader.CommitMessages(ctx, m)
			continue
		}

		c.log.Info("received build request", "submission_id", msg.SubmissionID)
		c.handler.Run(ctx, msg)

		if err := c.reader.CommitMessages(ctx, m); err != nil {
			c.log.Warn("commit failed", "error", err)
		}
	}
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}

func parseBrokers(brokers string) []string {
	parts := strings.Split(brokers, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
