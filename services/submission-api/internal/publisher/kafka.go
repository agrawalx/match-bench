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

const topicBuildRequested = "submission.build.requested"

type Publisher interface {
	PublishBuildRequested(ctx context.Context, meta PublishMeta) error
	Close() error
}

// PublishMeta carries all fields needed for the Kafka message.
type PublishMeta struct {
	SubmissionID string
	SHA256       string
	Language     string
	Protocol     string
	Port         int
	TeamName     string
	ArtifactPath string
	RequestedAt  time.Time
}

type KafkaPublisher struct {
	writer *kafka.Writer
	log    *slog.Logger
	noop   bool
}

// NewKafkaPublisher returns a no-op publisher when brokers is empty.
func NewKafkaPublisher(brokers string, log *slog.Logger) *KafkaPublisher {
	if brokers == "" {
		log.Warn("KAFKA_BROKERS not set — Kafka publishing disabled")
		return &KafkaPublisher{noop: true, log: log}
	}

	w := &kafka.Writer{
		Addr:         kafka.TCP(brokers),
		Topic:        topicBuildRequested,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireOne,
		Async:        false,
	}

	return &KafkaPublisher{writer: w, log: log}
}

func (p *KafkaPublisher) PublishBuildRequested(ctx context.Context, meta PublishMeta) error {
	if p.noop {
		return nil
	}

	msg := topics.SubmissionBuildRequested{
		SubmissionID: meta.SubmissionID,
		ContestantID: "",
		ArtifactPath: meta.ArtifactPath,
		Language:     meta.Language,
		Protocol:     meta.Protocol,
		Port:         meta.Port,
		TeamName:     meta.TeamName,
		SHA256:       meta.SHA256,
		RequestedAt:  meta.RequestedAt,
	}

	payload, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal kafka message: %w", err)
	}

	return p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(meta.SubmissionID),
		Value: payload,
	})
}

func (p *KafkaPublisher) Close() error {
	if p.noop || p.writer == nil {
		return nil
	}
	return p.writer.Close()
}
