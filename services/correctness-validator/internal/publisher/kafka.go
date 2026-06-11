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
	"strings"
	"time"

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
func New(brokers string) *Publisher {
	list := parseBrokers(brokers)
	return &Publisher{
		writer: &kafka.Writer{
			Addr:                   kafka.TCP(list...),
			Topic:                  topics.TopicScoresCorrectness,
			Balancer:               &kafka.LeastBytes{},
			RequiredAcks:           kafka.RequireAll,
			AllowAutoTopicCreation: false,
			WriteTimeout:           10 * time.Second,
		},
	}
}

// Close applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (p *Publisher) Close() error { return p.writer.Close() }

// Publish applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
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

// parseBrokers performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func parseBrokers(s string) []string {
	var out []string
	for _, b := range strings.Split(s, ",") {
		if b = strings.TrimSpace(b); b != "" {
			out = append(out, b)
		}
	}
	return out
}
