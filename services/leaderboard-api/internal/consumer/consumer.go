package consumer

import (
	"context"
	"encoding/json"
	"errors"
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
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        c.brokers,
		GroupID:        c.group,
		Topic:          topics.TopicLeaderboardUpdates,
		CommitInterval: 0,
		MinBytes:       1,
		MaxBytes:       1 << 20,
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
			if isDecodeError(err) {
				if err := r.CommitMessages(ctx, msg); err != nil {
					c.log.Warn("commit malformed leaderboard update failed", "error", err)
				}
			}
		} else {
			c.broker.Broadcast(ev)
			if err := r.CommitMessages(ctx, msg); err != nil {
				result = "error"
				c.log.Warn("commit leaderboard update failed", "error", err)
			}
		}
		metrics.Counter("leaderboard_api_events_consumed_total", "Leaderboard update events consumed by result.", metrics.Labels("result", result), 1)
		metrics.Counter("kafka_messages_consumed_total", "Kafka messages consumed by topic and result.", metrics.Labels("service", "leaderboard-api", "topic", topics.TopicLeaderboardUpdates, "result", result), 1)
	}
}

func isDecodeError(err error) bool {
	if err == nil {
		return false
	}
	var syntaxErr *json.SyntaxError
	if errors.As(err, &syntaxErr) {
		return true
	}
	var typeErr *json.UnmarshalTypeError
	return errors.As(err, &typeErr)
}
