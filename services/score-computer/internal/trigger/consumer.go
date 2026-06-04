package trigger

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/iicpc/libs/metrics"
	"github.com/iicpc/schemas/topics"
	"github.com/iicpc/score-computer/internal/store"
	"github.com/segmentio/kafka-go"
)

type Consumer struct {
	brokers []string
	store   *store.Store
	ready   chan<- string
	log     *slog.Logger
}

func New(brokers []string, st *store.Store, ready chan<- string, log *slog.Logger) *Consumer {
	return &Consumer{brokers: brokers, store: st, ready: ready, log: log}
}

func (c *Consumer) RunStatus(ctx context.Context, group string) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        c.brokers,
		GroupID:        group,
		Topic:          topics.TopicBenchmarkStatusUpdated,
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
			c.log.Warn("fetch benchmark status failed", "error", err)
			continue
		}
		start := time.Now()
		var ev topics.BenchmarkStatusUpdated
		err = json.Unmarshal(msg.Value, &ev)
		if err == nil {
			var ready []string
			ready, err = c.store.RecordStatus(ctx, ev)
			for _, id := range ready {
				c.enqueue(ctx, id)
			}
		}
		c.record(topics.TopicBenchmarkStatusUpdated, "status", start, err)
		if err == nil || isDecodeError(err) {
			err = r.CommitMessages(ctx, msg)
		}
		c.recordCommit(topics.TopicBenchmarkStatusUpdated, err)
		if err != nil {
			c.log.Warn("status message processing failed", "error", err)
		}
	}
}

func (c *Consumer) RunCorrectness(ctx context.Context, group string) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        c.brokers,
		GroupID:        group,
		Topic:          topics.TopicScoresCorrectness,
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
			c.log.Warn("fetch correctness score failed", "error", err)
			continue
		}
		start := time.Now()
		var ev topics.CorrectnessScoreEvent
		err = json.Unmarshal(msg.Value, &ev)
		if err == nil {
			var ready []string
			ready, err = c.store.RecordCorrectness(ctx, ev)
			for _, id := range ready {
				c.enqueue(ctx, id)
			}
		}
		c.record(topics.TopicScoresCorrectness, "correctness", start, err)
		if err == nil || isDecodeError(err) {
			err = r.CommitMessages(ctx, msg)
		}
		c.recordCommit(topics.TopicScoresCorrectness, err)
		if err != nil {
			c.log.Warn("correctness message processing failed", "error", err)
		}
	}
}

func (c *Consumer) enqueue(ctx context.Context, runGroupID string) {
	select {
	case c.ready <- runGroupID:
	case <-ctx.Done():
	}
}

func (c *Consumer) record(topic, source string, start time.Time, err error) {
	result := "ok"
	if err != nil {
		result = "error"
	}
	metrics.Counter("scorer_progress_events_total", "Scorer progress events persisted by source and result.", metrics.Labels("source", source, "result", result), 1)
	labels := metrics.Labels("service", "score-computer", "topic", topic, "result", result)
	metrics.Counter("kafka_messages_consumed_total", "Kafka messages consumed by topic and result.", labels, 1)
	metrics.Histogram("kafka_message_process_duration_seconds", "Kafka message processing duration in seconds.", labels, metrics.SinceSeconds(start))
}

func (c *Consumer) recordCommit(topic string, err error) {
	result := "ok"
	if err != nil {
		result = "error"
	}
	metrics.Counter("kafka_consumer_commit_total", "Kafka consumer commits by topic and result.", metrics.Labels("service", "score-computer", "topic", topic, "result", result), 1)
}

func DecodeStatus(value []byte) (topics.BenchmarkStatusUpdated, error) {
	var ev topics.BenchmarkStatusUpdated
	if err := json.Unmarshal(value, &ev); err != nil {
		return ev, fmt.Errorf("decode status: %w", err)
	}
	return ev, nil
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
