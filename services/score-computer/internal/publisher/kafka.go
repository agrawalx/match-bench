// Package publisher implements kafka behavior.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package publisher

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/iicpc/libs/metrics"
	"github.com/iicpc/schemas/topics"
	"github.com/segmentio/kafka-go"
)

// Publisher groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type Publisher struct {
	writer *kafka.Writer
}

// New performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func New(brokers []string) *Publisher {
	return &Publisher{writer: &kafka.Writer{
		Addr:                   kafka.TCP(brokers...),
		Topic:                  topics.TopicLeaderboardUpdates,
		Balancer:               &kafka.Hash{},
		RequiredAcks:           kafka.RequireAll,
		AllowAutoTopicCreation: false,
		WriteTimeout:           10 * time.Second,
	}}
}

// Close applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (p *Publisher) Close() error { return p.writer.Close() }

// Publish applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (p *Publisher) Publish(ctx context.Context, ev topics.LeaderboardUpdateEvent) error {
	start := time.Now()
	payload, err := json.Marshal(ev)
	if err == nil {
		err = p.writer.WriteMessages(ctx, kafka.Message{Key: []byte(ev.RunGroupID), Value: payload})
	}
	result := "ok"
	if err != nil {
		result = "error"
	}
	labels := metrics.Labels("service", "score-computer", "topic", topics.TopicLeaderboardUpdates, "result", result)
	metrics.Counter("kafka_messages_produced_total", "Kafka messages produced by topic and result.", labels, 1)
	metrics.Histogram("kafka_produce_duration_seconds", "Kafka produce duration in seconds.", labels, metrics.SinceSeconds(start))
	if err != nil {
		return fmt.Errorf("publish leaderboard update: %w", err)
	}
	return nil
}
