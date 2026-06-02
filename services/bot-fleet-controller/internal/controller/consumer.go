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

// Consumer owns the two inbound topics the controller cares about.
// benchmark.requested → spawn a new session runner.
// bot.ready           → demultiplex by session_id into the matching runner.
//
// Both readers are scoped to this single-replica controller (the service is
// architecturally locked at 1 pod, no sharding). One process means one
// consumer per topic; whatever partitions each topic has all balance to
// this pod. When/if the controller is ever multi-shard, bot.ready's
// session-keyed partitioning lets a future shard fan in only the sessions
// it owns — but that's a v2 problem; v1 is single-replica.
type Consumer struct {
	benchmarkReader *kafka.Reader
	botReadyReader  *kafka.Reader
	runner          *Runner
	sessions        *SessionManager
	log             *slog.Logger
}

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

func (c *Consumer) Close() {
	_ = c.benchmarkReader.Close()
	_ = c.botReadyReader.Close()
}

// StartBenchmarkRequested blocks consuming benchmark.requested.
//
// Sessions are processed SERIALLY: this loop fetches one message, runs the
// session to completion (Runner.Run is synchronous), commits the Kafka
// offset, and only then pulls the next message. With a single controller
// replica this gives every session exclusive use of the orchestrator + bot
// fleet, which is what we want for clean metrics and predictable resource
// usage.
//
// Within a run-group, submission-api publishes N benchmark.requested messages
// up front (one per scenario). Kafka delivers them; this loop drains them in
// publish order, producing the sequential per-group execution the load-test
// design requires. Cross-group serialization is a fortunate side-effect:
// only one benchmark runs anywhere in the cluster at a time. Lifting that
// limit is a v2 concern.
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

		// Synchronous: blocks until the session reaches a terminal state.
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

// StartBotReady blocks consuming bot.ready and demultiplexes by session_id.
// Unknown sessions (no entry in the session map) are committed and logged —
// they typically mean the message arrived after the session completed or on
// a controller restart.
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

// recordConsumer/recordControllerCommit expose the controller's inbound
// control topics.
func recordConsumer(topic, result string, durationSeconds float64) {
	labels := metrics.Labels("service", "bot-fleet-controller", "topic", topic, "result", result)
	metrics.Counter("kafka_messages_consumed_total", "Kafka messages consumed by topic and result.", labels, 1)
	if durationSeconds > 0 {
		metrics.Histogram("kafka_message_process_duration_seconds", "Kafka message processing duration in seconds.", labels, durationSeconds)
	}
}

func recordControllerCommit(topic string, err error) {
	result := "ok"
	if err != nil {
		result = "error"
	}
	metrics.Counter("kafka_consumer_commit_total", "Kafka consumer commits by topic and result.", metrics.Labels("service", "bot-fleet-controller", "topic", topic, "result", result), 1)
}
