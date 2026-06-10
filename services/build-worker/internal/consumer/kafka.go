// Package consumer implements kafka behavior.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package consumer

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"time"

	"github.com/iicpc/libs/metrics"
	"github.com/iicpc/schemas/topics"
	kafka "github.com/segmentio/kafka-go"
)

// Handler defines the behavior expected by this package boundary.
// Implementations should preserve the caller-visible contract.
type Handler interface {
	Run(ctx context.Context, msg topics.SubmissionBuildRequested)
}

// Consumer groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type Consumer struct {
	reader  *kafka.Reader
	handler Handler
	log     *slog.Logger
}

// NewKafkaConsumer performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func NewKafkaConsumer(brokers, groupID string, handler Handler, log *slog.Logger) *Consumer {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        parseBrokers(brokers),
		GroupID:        groupID,
		Topic:          topics.TopicSubmissionBuildRequested,
		MinBytes:       1,
		MaxBytes:       1 << 20,
		MaxWait:        100 * time.Millisecond,
		CommitInterval: 0,
		StartOffset:    kafka.FirstOffset,
	})
	return &Consumer{reader: r, handler: handler, log: log}
}

// Start applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (c *Consumer) Start(ctx context.Context) {
	c.log.Info("consumer started", "topic", topics.TopicSubmissionBuildRequested)
	for {
		m, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return // shutdown
			}
			recordConsume("fetch_error", 0)
			c.log.Error("fetch message failed", "error", err)
			continue
		}
		start := time.Now()

		var msg topics.SubmissionBuildRequested
		if err := json.Unmarshal(m.Value, &msg); err != nil {
			recordConsume("decode_error", metrics.SinceSeconds(start))
			c.log.Error("unmarshal failed", "error", err)
			recordCommit(c.commitMessage(m))
			continue
		}

		c.log.Info("received build request", "submission_id", msg.SubmissionID)
		c.handler.Run(ctx, msg)
		recordConsume("ok", metrics.SinceSeconds(start))

		if err := c.commitMessage(m); err != nil {
			recordCommit(err)
			c.log.Warn("commit failed", "error", err)
		} else {
			recordCommit(nil)
		}
	}
}

// commitMessage records that a fetched message has been handled. It uses a
// bounded background context so shutdown cannot cancel a commit after work ran.
func (c *Consumer) commitMessage(m kafka.Message) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return c.reader.CommitMessages(ctx, m)
}

// Close applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (c *Consumer) Close() error {
	return c.reader.Close()
}

// recordConsume performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func recordConsume(result string, durationSeconds float64) {
	labels := metrics.Labels("service", "build-worker", "topic", topics.TopicSubmissionBuildRequested, "result", result)
	metrics.Counter("kafka_messages_consumed_total", "Kafka messages consumed by topic and result.", labels, 1)
	if durationSeconds > 0 {
		metrics.Histogram("kafka_message_process_duration_seconds", "Kafka message processing duration in seconds.", labels, durationSeconds)
	}
}

// recordCommit performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func recordCommit(err error) {
	result := "ok"
	if err != nil {
		result = "error"
	}
	metrics.Counter("kafka_consumer_commit_total", "Kafka consumer commits by topic and result.", metrics.Labels("service", "build-worker", "topic", topics.TopicSubmissionBuildRequested, "result", result), 1)
}

// parseBrokers performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
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
