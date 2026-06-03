package controller

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
			Addr:  addr,
			Topic: topic,
			// Hash the message Key (session_id / session_id:worker_index) so a
			// session deterministically maps to one partition and its events stay
			// ordered. LeastBytes ignores the Key and scatters a session across
			// partitions — fine at 1 partition, but it breaks per-session ordering
			// (e.g. status walk applied out of order) the moment these topics are
			// created with >1 partition. Matches submission-api's keyed writer.
			Balancer:               &kafka.Hash{},
			RequiredAcks:           kafka.RequireAll,
			Async:                  false,
			AllowAutoTopicCreation: false,
			WriteTimeout:           writerTimeout,
		}
	}
	return &Producer{
		workloadWriter: mk(topics.TopicWorkloadAssignments),
		barrierWriter:  mk(topics.TopicBarrier),
		statusWriter:   mk(topics.TopicBenchmarkStatusUpdated),
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
	start := time.Now()
	msgs := make([]kafka.Message, 0, len(specs))
	for _, spec := range specs {
		payload, err := json.Marshal(spec)
		if err != nil {
			recordProduce(topics.TopicWorkloadAssignments, start, err)
			return fmt.Errorf("marshal workload spec: %w", err)
		}
		msgs = append(msgs, kafka.Message{
			Key:   []byte(fmt.Sprintf("%s:%d", spec.SessionID, spec.WorkerIndex)),
			Value: payload,
		})
	}
	writeCtx, cancel := context.WithTimeout(ctx, writerTimeout)
	defer cancel()

	err := p.workloadWriter.WriteMessages(writeCtx, msgs...)
	recordProduce(topics.TopicWorkloadAssignments, start, err)
	return err
}

// PublishBarrier publishes one BarrierEvent. Keyed by session_id so all
// workers (which subscribe with unique per-worker consumer groups) read the
// same event.
func (p *Producer) PublishBarrier(ctx context.Context, sessionID string, targetEpochNS uint64) error {
	if p.noop {
		return nil
	}
	start := time.Now()
	payload, err := json.Marshal(topics.BarrierEvent{
		SessionID:            sessionID,
		TargetEpochUnixNanos: targetEpochNS,
	})
	if err != nil {
		recordProduce(topics.TopicBarrier, start, err)
		return fmt.Errorf("marshal barrier: %w", err)
	}
	writeCtx, cancel := context.WithTimeout(ctx, writerTimeout)
	defer cancel()

	err = p.barrierWriter.WriteMessages(writeCtx, kafka.Message{
		Key:   []byte(sessionID),
		Value: payload,
	})
	recordProduce(topics.TopicBarrier, start, err)
	return err
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
	start := time.Now()
	payload, err := json.Marshal(evt)
	if err != nil {
		recordProduce(topics.TopicBenchmarkStatusUpdated, start, err)
		return fmt.Errorf("marshal status: %w", err)
	}
	writeCtx, cancel := context.WithTimeout(ctx, writerTimeout)
	defer cancel()

	err = p.statusWriter.WriteMessages(writeCtx, kafka.Message{
		Key:   []byte(evt.SessionID),
		Value: payload,
	})
	recordProduce(topics.TopicBenchmarkStatusUpdated, start, err)
	return err
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

// recordProduce measures controller-owned control-plane topics.
func recordProduce(topic string, start time.Time, err error) {
	result := "ok"
	if err != nil {
		result = "error"
	}
	labels := metrics.Labels("service", "bot-fleet-controller", "topic", topic, "result", result)
	metrics.Counter("kafka_messages_produced_total", "Kafka messages produced by topic and result.", labels, 1)
	metrics.Histogram("kafka_produce_duration_seconds", "Kafka produce duration in seconds.", labels, metrics.SinceSeconds(start))
}
