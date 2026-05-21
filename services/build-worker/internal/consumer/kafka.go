package consumer

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/iicpc/schemas/topics"
	kafka "github.com/segmentio/kafka-go"
)

const topicBuildRequested = "submission.build.requested"

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
		Brokers: []string{brokers},
		GroupID: groupID,
		Topic:   topicBuildRequested,
	})
	return &Consumer{reader: r, handler: handler, log: log}
}

// Start blocks and processes messages until ctx is cancelled.
func (c *Consumer) Start(ctx context.Context) {
	c.log.Info("consumer started", "topic", topicBuildRequested)
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
