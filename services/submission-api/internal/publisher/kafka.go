// Package publisher implements kafka behavior.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package publisher

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/iicpc/libs/metrics"
	"github.com/iicpc/schemas/topics"
	cerrs "github.com/iicpc/submission-api/internal/errors"
	"github.com/iicpc/submission-api/internal/utils"
	kafka "github.com/segmentio/kafka-go"
)

const writerTimeout = 5 * time.Second

// Publisher defines the behavior expected by this package boundary.
// Implementations should preserve the caller-visible contract.
type Publisher interface {
	PublishBuildRequested(ctx context.Context, meta PublishMeta) error
	PublishBenchmarkRequested(ctx context.Context, meta BenchmarkMeta) error
	Close() error
}

// PublishMeta groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type PublishMeta struct {
	SubmissionID string
	ContestantID string
	SHA256       string
	Language     string
	Protocol     string
	Port         int
	BuildType    string
	BuildTarget  string
	TeamName     string
	ArtifactPath string
	RequestedAt  time.Time
}

// BenchmarkMeta groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type BenchmarkMeta struct {
	SessionID    string
	SubmissionID string
	ContestantID string
	RunGroupID   string
	ScenarioID   string
	RequestedAt  time.Time
}

// KafkaPublisher groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type KafkaPublisher struct {
	buildWriter     *kafka.Writer
	benchmarkWriter *kafka.Writer
	log             *slog.Logger
	noop            bool
}

// NewKafkaPublisher performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func NewKafkaPublisher(brokers string, log *slog.Logger) *KafkaPublisher {
	brokerList := utils.ParseBrokers(brokers)
	if len(brokerList) == 0 {
		log.Warn("KAFKA_BROKERS not set — Kafka publishing disabled")
		return &KafkaPublisher{noop: true, log: log}
	}

	addr := kafka.TCP(brokerList...)

	build := &kafka.Writer{
		Addr:                   addr,
		Topic:                  topics.TopicSubmissionBuildRequested,
		Balancer:               &kafka.Hash{},
		RequiredAcks:           kafka.RequireOne,
		Async:                  false,
		AllowAutoTopicCreation: false,
		WriteTimeout:           writerTimeout,
	}

	bench := &kafka.Writer{
		Addr:                   addr,
		Topic:                  topics.TopicBenchmarkRequested,
		Balancer:               &kafka.Hash{},
		RequiredAcks:           kafka.RequireAll,
		Async:                  false,
		AllowAutoTopicCreation: false,
		WriteTimeout:           writerTimeout,
	}

	return &KafkaPublisher{buildWriter: build, benchmarkWriter: bench, log: log}
}

// PublishBuildRequested applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (p *KafkaPublisher) PublishBuildRequested(ctx context.Context, meta PublishMeta) error {
	if p.noop {
		return nil
	}
	start := time.Now()

	msg := topics.SubmissionBuildRequested{
		SubmissionID: meta.SubmissionID,
		ContestantID: meta.ContestantID,
		ArtifactPath: meta.ArtifactPath,
		Language:     meta.Language,
		Protocol:     meta.Protocol,
		Port:         meta.Port,
		BuildType:    meta.BuildType,
		BuildTarget:  meta.BuildTarget,
		TeamName:     meta.TeamName,
		SHA256:       meta.SHA256,
		RequestedAt:  meta.RequestedAt,
	}

	payload, err := json.Marshal(msg)
	if err != nil {
		recordProduce(topics.TopicSubmissionBuildRequested, start, err)
		return fmt.Errorf("%w: marshal kafka message: %v", cerrs.ErrInternal, err)
	}

	writeCtx, cancel := context.WithTimeout(ctx, writerTimeout)
	defer cancel()

	err = p.buildWriter.WriteMessages(writeCtx, kafka.Message{
		Key:   []byte(meta.SubmissionID),
		Value: payload,
	})
	recordProduce(topics.TopicSubmissionBuildRequested, start, err)
	return err
}

// PublishBenchmarkRequested applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (p *KafkaPublisher) PublishBenchmarkRequested(ctx context.Context, meta BenchmarkMeta) error {
	if p.noop {
		return nil
	}
	start := time.Now()

	msg := topics.BenchmarkRequested{
		SessionID:    meta.SessionID,
		SubmissionID: meta.SubmissionID,
		ContestantID: meta.ContestantID,
		RunGroupID:   meta.RunGroupID,
		ScenarioID:   meta.ScenarioID,
		RequestedAt:  meta.RequestedAt,
	}

	payload, err := json.Marshal(msg)
	if err != nil {
		recordProduce(topics.TopicBenchmarkRequested, start, err)
		return fmt.Errorf("%w: marshal benchmark request: %v", cerrs.ErrInternal, err)
	}

	writeCtx, cancel := context.WithTimeout(ctx, writerTimeout)
	defer cancel()

	err = p.benchmarkWriter.WriteMessages(writeCtx, kafka.Message{
		Key:   benchmarkMessageKey(meta),
		Value: payload,
	})
	recordProduce(topics.TopicBenchmarkRequested, start, err)
	return err
}

// benchmarkMessageKey performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func benchmarkMessageKey(meta BenchmarkMeta) []byte {
	if meta.RunGroupID != "" {
		return []byte(meta.RunGroupID)
	}
	return []byte(meta.SessionID)
}

// Close applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (p *KafkaPublisher) Close() error {
	if p.noop {
		return nil
	}
	var firstErr error
	if p.buildWriter != nil {
		if err := p.buildWriter.Close(); err != nil {
			firstErr = err
			p.log.Warn("build kafka writer close failed", "error", err)
		}
	}
	if p.benchmarkWriter != nil {
		if err := p.benchmarkWriter.Close(); err != nil {
			p.log.Warn("benchmark kafka writer close failed", "error", err)
			firstErr = errors.Join(firstErr, err)
		}
	}
	return firstErr
}

// recordProduce performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func recordProduce(topic string, start time.Time, err error) {
	result := "ok"
	if err != nil {
		result = "error"
	}
	labels := metrics.Labels("service", "submission-api", "topic", topic, "result", result)
	metrics.Counter("kafka_messages_produced_total", "Kafka messages produced by topic and result.", labels, 1)
	metrics.Histogram("kafka_produce_duration_seconds", "Kafka produce duration in seconds.", labels, metrics.SinceSeconds(start))
}
