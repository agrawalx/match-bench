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
	"log/slog"
	"strings"
	"time"

	"github.com/iicpc/libs/metrics"
	"github.com/iicpc/schemas/topics"
	kafka "github.com/segmentio/kafka-go"
)

const writerTimeout = 5 * time.Second

// Publisher groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type Publisher struct {
	writer *kafka.Writer
	log    *slog.Logger
	noop   bool
}

// NewKafkaPublisher performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func NewKafkaPublisher(brokers string, log *slog.Logger) *Publisher {
	if brokers == "" {
		log.Warn("KAFKA_BROKERS not set — status publishing disabled")
		return &Publisher{noop: true, log: log}
	}
	brokerList := parseBrokers(brokers)
	if len(brokerList) == 0 {
		log.Warn("KAFKA_BROKERS contains no usable brokers — status publishing disabled")
		return &Publisher{noop: true, log: log}
	}

	w := &kafka.Writer{
		Addr:                   kafka.TCP(brokerList...),
		Topic:                  topics.TopicSubmissionStatusUpdated,
		Balancer:               &kafka.LeastBytes{},
		RequiredAcks:           kafka.RequireOne,
		Async:                  false,
		AllowAutoTopicCreation: false,
		WriteTimeout:           writerTimeout,
	}

	return &Publisher{writer: w, log: log}
}

// PublishStatus applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (p *Publisher) PublishStatus(ctx context.Context, submissionID, status, message string) error {
	if p.noop {
		return nil
	}
	start := time.Now()

	payload, err := json.Marshal(topics.SubmissionStatusUpdated{
		SubmissionID: submissionID,
		Status:       status,
		Message:      message,
		UpdatedAt:    time.Now().UTC(),
	})
	if err != nil {
		recordProduce(start, err)
		return fmt.Errorf("marshal status update: %w", err)
	}

	writeCtx, cancel := context.WithTimeout(ctx, writerTimeout)
	defer cancel()

	err = p.writer.WriteMessages(writeCtx, kafka.Message{
		Key:   []byte(submissionID),
		Value: payload,
	})
	recordProduce(start, err)
	return err
}

// Close applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (p *Publisher) Close() error {
	if p.noop || p.writer == nil {
		return nil
	}
	return p.writer.Close()
}

// recordProduce performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func recordProduce(start time.Time, err error) {
	result := "ok"
	if err != nil {
		result = "error"
	}
	labels := metrics.Labels("service", "build-worker", "topic", topics.TopicSubmissionStatusUpdated, "result", result)
	metrics.Counter("kafka_messages_produced_total", "Kafka messages produced by topic and result.", labels, 1)
	metrics.Histogram("kafka_produce_duration_seconds", "Kafka produce duration in seconds.", labels, metrics.SinceSeconds(start))
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
