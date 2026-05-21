package publisher

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/iicpc/schemas/topics"
	kafka "github.com/segmentio/kafka-go"
)

const topicStatusUpdated = "submission.status.updated"

type Publisher struct {
	writer *kafka.Writer
	log    *slog.Logger
	noop   bool
}

func NewKafkaPublisher(brokers string, log *slog.Logger) *Publisher {
	if brokers == "" {
		log.Warn("KAFKA_BROKERS not set — status publishing disabled")
		return &Publisher{noop: true, log: log}
	}

	w := &kafka.Writer{
		Addr:         kafka.TCP(brokers),
		Topic:        topicStatusUpdated,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireOne,
		Async:        false,
	}

	return &Publisher{writer: w, log: log}
}

func (p *Publisher) PublishStatus(ctx context.Context, submissionID, status, message string) error {
	if p.noop {
		return nil
	}

	payload, err := json.Marshal(topics.SubmissionStatusUpdated{
		SubmissionID: submissionID,
		Status:       status,
		Message:      message,
		UpdatedAt:    time.Now().UTC(),
	})
	if err != nil {
		return fmt.Errorf("marshal status update: %w", err)
	}

	return p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(submissionID),
		Value: payload,
	})
}

func (p *Publisher) Close() error {
	if p.noop || p.writer == nil {
		return nil
	}
	return p.writer.Close()
}
