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

type Consumer struct {
	brokers []string
	group   string
	broker  *sse.Broker
	log     *slog.Logger
}

func New(brokers []string, group string, broker *sse.Broker, log *slog.Logger) *Consumer {
	return &Consumer{brokers: brokers, group: group, broker: broker, log: log}
}

func (c *Consumer) Run(ctx context.Context) {
	// c.group is unique per pod (POD_NAME suffix) so every replica receives
	// every update — the SSE broker only fans out to its own clients. These
	// groups are transient fan-out groups: offsets are never committed (there
	// is no resume semantic — the snapshot endpoint covers reconnect state) and
	// StartOffset=LastOffset attaches a fresh pod at the log tail instead of
	// replaying stale updates. With no committed offsets, empty groups are
	// garbage-collected by the broker instead of accumulating per pod churn.
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
