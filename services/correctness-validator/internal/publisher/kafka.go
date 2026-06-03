// Package publisher emits CorrectnessScoreEvent (JSON) to scores.correctness.
package publisher

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/iicpc/schemas/topics"
	"github.com/segmentio/kafka-go"
)

type Publisher struct {
	writer *kafka.Writer
}

func New(brokers string) *Publisher {
	list := parseBrokers(brokers)
	return &Publisher{
		writer: &kafka.Writer{
			Addr:                   kafka.TCP(list...),
			Topic:                  topics.TopicScoresCorrectness,
			Balancer:               &kafka.LeastBytes{},
			RequiredAcks:           kafka.RequireAll, // control-plane durability
			AllowAutoTopicCreation: false,
			WriteTimeout:           10 * time.Second,
		},
	}
}

func (p *Publisher) Close() error { return p.writer.Close() }

func (p *Publisher) Publish(ctx context.Context, ev topics.CorrectnessScoreEvent) error {
	payload, err := json.Marshal(ev)
	if err != nil {
		return fmt.Errorf("marshal correctness score: %w", err)
	}
	return p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(ev.SessionID),
		Value: payload,
	})
}

func parseBrokers(s string) []string {
	var out []string
	for _, b := range strings.Split(s, ",") {
		if b = strings.TrimSpace(b); b != "" {
			out = append(out, b)
		}
	}
	return out
}
