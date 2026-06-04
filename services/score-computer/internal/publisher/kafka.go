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

type Publisher struct {
	writer *kafka.Writer
}

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

func (p *Publisher) Close() error { return p.writer.Close() }

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
