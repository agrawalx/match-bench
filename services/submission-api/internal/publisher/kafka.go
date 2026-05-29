package publisher

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/iicpc/schemas/topics"
	cerrs "github.com/iicpc/submission-api/internal/errors"
	"github.com/iicpc/submission-api/internal/utils"
	kafka "github.com/segmentio/kafka-go"
)

const writerTimeout = 5 * time.Second

type Publisher interface {
	PublishBuildRequested(ctx context.Context, meta PublishMeta) error
	PublishBenchmarkRequested(ctx context.Context, meta BenchmarkMeta) error
	Close() error
}

// PublishMeta carries all fields needed for the build.requested Kafka message.
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

// BenchmarkMeta carries the fields needed for benchmark.requested.
//
// session_id is minted by the submission-api handler as a UUID v7 (so it is
// both globally unique and time-ordered, which is useful for log scans).
// The handler must return run_id synchronously in the HTTP response so the
// frontend can start polling — that's why minting happens in the API
// instead of being delegated to the controller.
//
// One "click benchmark" expands into multiple BenchmarkMeta values — one per
// scenario in the scenarios table. All share the same RunGroupID; each
// points at a different ScenarioID. The controller looks up the scenario
// row when the message arrives.
type BenchmarkMeta struct {
	SessionID    string
	SubmissionID string
	ContestantID string
	RunGroupID   string
	ScenarioID   string
	RequestedAt  time.Time
}

type KafkaPublisher struct {
	buildWriter     *kafka.Writer
	benchmarkWriter *kafka.Writer
	log             *slog.Logger
	noop            bool
}

// NewKafkaPublisher returns a no-op publisher when brokers is empty.
// In no-op mode publish calls return nil after intentionally discarding the
// event; callers must not treat nil as proof of Kafka delivery in local dev.
func NewKafkaPublisher(brokers string, log *slog.Logger) *KafkaPublisher {
	brokerList := utils.ParseBrokers(brokers)
	if len(brokerList) == 0 {
		log.Warn("KAFKA_BROKERS not set — Kafka publishing disabled")
		return &KafkaPublisher{noop: true, log: log}
	}

	addr := kafka.TCP(brokerList...)

	// Build requests are recoverable from the submissions row and build-worker
	// retry model, so this path uses leader-only acks to keep upload latency
	// lower than the benchmark control-plane path.
	build := &kafka.Writer{
		Addr:                   addr,
		Topic:                  topics.TopicSubmissionBuildRequested,
		Balancer:               &kafka.LeastBytes{},
		RequiredAcks:           kafka.RequireOne,
		Async:                  false,
		AllowAutoTopicCreation: false,
		WriteTimeout:           writerTimeout,
	}

	// Synchronous publish for benchmark.requested: the user got a run_id back
	// in the HTTP response, and if the message is dropped the controller will
	// never see it. Losing this silently strands the run in 'requested'.
	bench := &kafka.Writer{
		Addr:                   addr,
		Topic:                  topics.TopicBenchmarkRequested,
		Balancer:               &kafka.LeastBytes{},
		RequiredAcks:           kafka.RequireAll,
		Async:                  false,
		AllowAutoTopicCreation: false,
		WriteTimeout:           writerTimeout,
	}

	return &KafkaPublisher{buildWriter: build, benchmarkWriter: bench, log: log}
}

func (p *KafkaPublisher) PublishBuildRequested(ctx context.Context, meta PublishMeta) error {
	if p.noop {
		return nil
	}

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
		return fmt.Errorf("%w: marshal kafka message: %v", cerrs.ErrInternal, err)
	}

	return p.buildWriter.WriteMessages(ctx, kafka.Message{
		Key:   []byte(meta.SubmissionID),
		Value: payload,
	})
}

func (p *KafkaPublisher) PublishBenchmarkRequested(ctx context.Context, meta BenchmarkMeta) error {
	if p.noop {
		return nil
	}

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
		return fmt.Errorf("%w: marshal benchmark request: %v", cerrs.ErrInternal, err)
	}

	return p.benchmarkWriter.WriteMessages(ctx, kafka.Message{
		Key:   []byte(meta.SessionID),
		Value: payload,
	})
}

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
