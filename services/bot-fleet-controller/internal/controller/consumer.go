package controller

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/iicpc/schemas/topics"
	kafka "github.com/segmentio/kafka-go"
)

const (
	TopicBenchmarkRequested = "benchmark.requested"
	TopicBotReady           = "bot.ready"
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
	return &Consumer{
		benchmarkReader: kafka.NewReader(kafka.ReaderConfig{
			Brokers: []string{brokers},
			GroupID: benchmarkGroup,
			Topic:   TopicBenchmarkRequested,
		}),
		botReadyReader: kafka.NewReader(kafka.ReaderConfig{
			Brokers: []string{brokers},
			GroupID: botReadyGroup,
			Topic:   TopicBotReady,
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
// Each valid message spawns a runner goroutine; the consumer returns once
// the message is committed (the runner runs independently).
func (c *Consumer) StartBenchmarkRequested(ctx context.Context) {
	c.log.Info("benchmark.requested consumer started")
	for {
		m, err := c.benchmarkReader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			c.log.Error("fetch benchmark.requested", "error", err)
			continue
		}

		var req topics.BenchmarkRequested
		if err := json.Unmarshal(m.Value, &req); err != nil {
			c.log.Error("unmarshal benchmark.requested", "error", err, "key", string(m.Key))
			_ = c.benchmarkReader.CommitMessages(ctx, m)
			continue
		}

		c.runner.Start(ctx, req)

		if err := c.benchmarkReader.CommitMessages(ctx, m); err != nil {
			c.log.Warn("commit benchmark.requested", "session_id", req.SessionID, "error", err)
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
			c.log.Error("fetch bot.ready", "error", err)
			continue
		}

		var sig topics.ReadySignal
		if err := json.Unmarshal(m.Value, &sig); err != nil {
			c.log.Error("unmarshal bot.ready", "error", err, "key", string(m.Key))
			_ = c.botReadyReader.CommitMessages(ctx, m)
			continue
		}

		if ok := c.sessions.DispatchReady(sig); !ok {
			c.log.Warn("ready signal for unknown session",
				"session_id", sig.SessionID,
				"worker_id", sig.WorkerID,
				"worker_index", sig.WorkerIndex,
			)
		}

		if err := c.botReadyReader.CommitMessages(ctx, m); err != nil {
			c.log.Warn("commit bot.ready", "session_id", sig.SessionID, "error", err)
		}
	}
}
