package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"

	"github.com/iicpc/schemas/topics"
	kafka "github.com/segmentio/kafka-go"
)

const (
	TopicWorkloadAssignments     = "workload.assignments"
	TopicBarrier                 = "barrier"
	TopicBenchmarkStatusUpdated  = "benchmark.status.updated"
)

// Producer publishes the three controller-owned topics. Each topic gets its
// own kafka.Writer because segmentio/kafka-go pins the topic on the writer.
//
// All three are control-plane events: RequiredAcks=All, synchronous. Loss
// equals a stuck run (no barrier fires, no status arrives), so we pay for
// durability.
type Producer struct {
	workloadWriter *kafka.Writer
	barrierWriter  *kafka.Writer
	statusWriter   *kafka.Writer
	log            *slog.Logger
	noop           bool
}

func NewProducer(brokers string, log *slog.Logger) *Producer {
	list := parseBrokers(brokers)
	if len(list) == 0 {
		log.Warn("KAFKA_BROKERS not set — controller publish disabled")
		return &Producer{noop: true, log: log}
	}
	addr := kafka.TCP(list...)
	mk := func(topic string) *kafka.Writer {
		return &kafka.Writer{
			Addr:                   addr,
			Topic:                  topic,
			Balancer:               &kafka.LeastBytes{},
			RequiredAcks:           kafka.RequireAll,
			Async:                  false,
			AllowAutoTopicCreation: true,
		}
	}
	return &Producer{
		workloadWriter: mk(TopicWorkloadAssignments),
		barrierWriter:  mk(TopicBarrier),
		statusWriter:   mk(TopicBenchmarkStatusUpdated),
		log:            log,
	}
}

func (p *Producer) Close() error {
	if p.noop {
		return nil
	}
	var firstErr error
	for _, w := range []*kafka.Writer{p.workloadWriter, p.barrierWriter, p.statusWriter} {
		if w == nil {
			continue
		}
		if err := w.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// PublishWorkloadSpec publishes one WorkloadSpec per worker_index, keyed by
// session_id:worker_index. With proper partition sizing each worker pod
// consumes exactly one spec.
func (p *Producer) PublishWorkloadSpec(ctx context.Context, specs []topics.WorkloadSpec) error {
	if p.noop {
		return nil
	}
	msgs := make([]kafka.Message, 0, len(specs))
	for _, spec := range specs {
		payload, err := json.Marshal(spec)
		if err != nil {
			return fmt.Errorf("marshal workload spec: %w", err)
		}
		msgs = append(msgs, kafka.Message{
			Key:   []byte(fmt.Sprintf("%s:%d", spec.SessionID, spec.WorkerIndex)),
			Value: payload,
		})
	}
	return p.workloadWriter.WriteMessages(ctx, msgs...)
}

// PublishBarrier publishes one BarrierEvent. Keyed by session_id so all
// workers (which subscribe with unique per-worker consumer groups) read the
// same event.
func (p *Producer) PublishBarrier(ctx context.Context, sessionID string, targetEpochNS uint64) error {
	if p.noop {
		return nil
	}
	payload, err := json.Marshal(topics.BarrierEvent{
		SessionID:            sessionID,
		TargetEpochUnixNanos: targetEpochNS,
	})
	if err != nil {
		return fmt.Errorf("marshal barrier: %w", err)
	}
	return p.barrierWriter.WriteMessages(ctx, kafka.Message{
		Key:   []byte(sessionID),
		Value: payload,
	})
}

// PublishStatus publishes one benchmark.status.updated event. submission-api
// consumes this and updates the runs row.
//
// This is the ONLY path through which terminal status ('completed', 'failed')
// reaches PostgreSQL during normal operation. The hard rule is that the
// controller is the sole producer of these events, and submission-api's
// consumer is the sole writer of the terminal column value. The single
// documented exception is the controller's startup recovery, which writes
// 'failed' to runs directly (synchronously) before any consumer is alive.
func (p *Producer) PublishStatus(ctx context.Context, evt topics.BenchmarkStatusUpdated) error {
	if p.noop {
		return nil
	}
	payload, err := json.Marshal(evt)
	if err != nil {
		return fmt.Errorf("marshal status: %w", err)
	}
	return p.statusWriter.WriteMessages(ctx, kafka.Message{
		Key:   []byte(evt.SessionID),
		Value: payload,
	})
}

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
