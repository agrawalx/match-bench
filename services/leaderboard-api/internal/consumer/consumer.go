// Package consumer implements consumer behavior.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package consumer

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/iicpc/leaderboard-api/internal/sse"
	"github.com/iicpc/libs/metrics"
	"github.com/iicpc/schemas/topics"
	"github.com/segmentio/kafka-go"
)

// Consumer groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type Consumer struct {
	brokers []string
	group   string
	broker  *sse.Broker
	log     *slog.Logger
}

// New performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func New(brokers []string, group string, broker *sse.Broker, log *slog.Logger) *Consumer {
	return &Consumer{brokers: brokers, group: group, broker: broker, log: log}
}

// Run applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (c *Consumer) Run(ctx context.Context) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     c.brokers,
		GroupID:     c.group,
		Topic:       topics.TopicLeaderboardUpdates,
		StartOffset: kafka.LastOffset,
		MinBytes:    1,
		MaxBytes:    1 << 20,
	})
	defer r.Close()
	for {
		msg, err := r.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			c.log.Warn("fetch leaderboard update failed", "error", err)
			continue
		}
		result := "ok"
		var ev topics.LeaderboardUpdateEvent
		if err := json.Unmarshal(msg.Value, &ev); err != nil {
			result = "error"
			c.log.Warn("decode leaderboard update failed", "error", err)
		} else {
			c.broker.Broadcast(ev)
		}
		metrics.Counter("leaderboard_api_events_consumed_total", "Leaderboard update events consumed by result.", metrics.Labels("result", result), 1)
		metrics.Counter("kafka_messages_consumed_total", "Kafka messages consumed by topic and result.", metrics.Labels("service", "leaderboard-api", "topic", topics.TopicLeaderboardUpdates, "result", result), 1)
	}
}
