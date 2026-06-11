// Package controller implements consumer behavior.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package controller

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/iicpc/libs/metrics"
	"github.com/iicpc/schemas/topics"
	kafka "github.com/segmentio/kafka-go"
)

// Consumer groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type Consumer struct {
	benchmarkReader *kafka.Reader
	botReadyReader  *kafka.Reader
	runner          *Runner
	sessions        *SessionManager
	log             *slog.Logger
}

// NewConsumer performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func NewConsumer(brokers, benchmarkGroup, botReadyGroup string, runner *Runner, sessions *SessionManager, log *slog.Logger) *Consumer {
	brokerList := parseBrokers(brokers)
	return &Consumer{
		benchmarkReader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:        brokerList,
			GroupID:        benchmarkGroup,
			Topic:          topics.TopicBenchmarkRequested,
			MinBytes:       1,
			MaxBytes:       1 << 20,
			MaxWait:        100 * time.Millisecond,
			CommitInterval: 0,
		}),
		botReadyReader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:        brokerList,
			GroupID:        botReadyGroup,
			Topic:          topics.TopicBotReady,
			MinBytes:       1,
			MaxBytes:       1 << 20,
			MaxWait:        100 * time.Millisecond,
			CommitInterval: 0,
		}),
		runner:   runner,
		sessions: sessions,
		log:      log,
	}
}

// Close applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (c *Consumer) Close() {
	_ = c.benchmarkReader.Close()
	_ = c.botReadyReader.Close()
}

// StartBenchmarkRequested applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (c *Consumer) StartBenchmarkRequested(ctx context.Context) {
	c.log.Info("benchmark.requested consumer started")
	for {
		m, err := c.benchmarkReader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			recordConsumer(topics.TopicBenchmarkRequested, "fetch_error", 0)
			c.log.Error("fetch benchmark.requested", "error", err)
			continue
		}
		start := time.Now()

		var req topics.BenchmarkRequested
		if err := json.Unmarshal(m.Value, &req); err != nil {
			recordConsumer(topics.TopicBenchmarkRequested, "decode_error", metrics.SinceSeconds(start))
			c.log.Error("unmarshal benchmark.requested", "error", err, "key", string(m.Key))
			recordControllerCommit(topics.TopicBenchmarkRequested, c.benchmarkReader.CommitMessages(ctx, m))
			continue
		}

		c.runner.Run(ctx, req)
		recordConsumer(topics.TopicBenchmarkRequested, "ok", metrics.SinceSeconds(start))

		if err := c.benchmarkReader.CommitMessages(ctx, m); err != nil {
			recordControllerCommit(topics.TopicBenchmarkRequested, err)
			c.log.Warn("commit benchmark.requested", "session_id", req.SessionID, "error", err)
		} else {
			recordControllerCommit(topics.TopicBenchmarkRequested, nil)
		}
	}
}

// StartBotReady applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (c *Consumer) StartBotReady(ctx context.Context) {
	c.log.Info("bot.ready consumer started")
	for {
		m, err := c.botReadyReader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			recordConsumer(topics.TopicBotReady, "fetch_error", 0)
			c.log.Error("fetch bot.ready", "error", err)
			continue
		}
		start := time.Now()

		var sig topics.ReadySignal
		if err := json.Unmarshal(m.Value, &sig); err != nil {
			recordConsumer(topics.TopicBotReady, "decode_error", metrics.SinceSeconds(start))
			c.log.Error("unmarshal bot.ready", "error", err, "key", string(m.Key))
			recordControllerCommit(topics.TopicBotReady, c.botReadyReader.CommitMessages(ctx, m))
			continue
		}

		if ok := c.sessions.DispatchReady(sig); !ok {
			metrics.Counter("controller_unknown_ready_signal_total", "Ready signals for unknown sessions.", nil, 1)
			c.log.Warn("ready signal for unknown session",
				"session_id", sig.SessionID,
				"worker_id", sig.WorkerID,
				"worker_index", sig.WorkerIndex,
			)
		}
		recordConsumer(topics.TopicBotReady, "ok", metrics.SinceSeconds(start))

		if err := c.botReadyReader.CommitMessages(ctx, m); err != nil {
			recordControllerCommit(topics.TopicBotReady, err)
			c.log.Warn("commit bot.ready", "session_id", sig.SessionID, "error", err)
		} else {
			recordControllerCommit(topics.TopicBotReady, nil)
		}
	}
}

// recordConsumer performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func recordConsumer(topic, result string, durationSeconds float64) {
	labels := metrics.Labels("service", "bot-fleet-controller", "topic", topic, "result", result)
	metrics.Counter("kafka_messages_consumed_total", "Kafka messages consumed by topic and result.", labels, 1)
	if durationSeconds > 0 {
		metrics.Histogram("kafka_message_process_duration_seconds", "Kafka message processing duration in seconds.", labels, durationSeconds)
	}
}

// recordControllerCommit performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func recordControllerCommit(topic string, err error) {
	result := "ok"
	if err != nil {
		result = "error"
	}
	metrics.Counter("kafka_consumer_commit_total", "Kafka consumer commits by topic and result.", metrics.Labels("service", "bot-fleet-controller", "topic", topic, "result", result), 1)
}
