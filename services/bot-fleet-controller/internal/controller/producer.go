// Package controller implements producer behavior.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
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

// Producer groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type Producer struct {
	workloadWriter *kafka.Writer
	barrierWriter  *kafka.Writer
	statusWriter   *kafka.Writer
	log            *slog.Logger
	noop           bool

	workloadPartitions int
}

// NewProducer performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func NewProducer(brokers string, log *slog.Logger) *Producer {
	list := parseBrokers(brokers)
	if len(list) == 0 {
		log.Warn("KAFKA_BROKERS not set — controller publish disabled")
		return &Producer{noop: true, log: log}
	}
	addr := kafka.TCP(list...)
	mk := func(topic string, balancer kafka.Balancer) *kafka.Writer {
		return &kafka.Writer{
			Addr:                   addr,
			Topic:                  topic,
			Balancer:               balancer,
			RequiredAcks:           kafka.RequireAll,
			Async:                  false,
			AllowAutoTopicCreation: false,
			WriteTimeout:           writerTimeout,
		}
	}
	return &Producer{
		workloadWriter: mk(topics.TopicWorkloadAssignments, &workerIndexBalancer{}),
		barrierWriter:  mk(topics.TopicBarrier, &kafka.Hash{}),
		statusWriter:   mk(topics.TopicBenchmarkStatusUpdated, &kafka.Hash{}),
		log:            log,
	}
}

// workerIndexBalancer groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type workerIndexBalancer struct {
	fallback kafka.Hash
}

// Balance applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (b *workerIndexBalancer) Balance(msg kafka.Message, partitions ...int) int {
	idx, ok := msg.WriterData.(uint32)
	if !ok {
		return b.fallback.Balance(msg, partitions...)
	}
	return partitions[workerPartition(idx, len(partitions))]
}

// workerPartition performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func workerPartition(workerIndex uint32, numPartitions int) int {
	if numPartitions <= 0 {
		return 0
	}
	return int(workerIndex % uint32(numPartitions))
}

// validateWorkerCapacity performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func validateWorkerCapacity(workerCount, partitions int) error {
	if partitions <= 0 {
		return fmt.Errorf("workload.assignments reports %d partitions; cannot assign workers", partitions)
	}
	if workerCount > partitions {
		return fmt.Errorf(
			"worker_count %d exceeds workload.assignments partition count %d: two specs would share a partition (serial execution, missed barrier); lower MAX_TASKS_PER_WORKER's resulting worker count or repartition the topic",
			workerCount, partitions,
		)
	}
	return nil
}

// Close applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
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

// PublishWorkloadSpec applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (p *Producer) PublishWorkloadSpec(ctx context.Context, specs []topics.WorkloadSpec) error {
	if p.noop {
		return nil
	}
	start := time.Now()
	partitions, err := p.workloadPartitionCount(ctx)
	if err != nil {
		recordProduce(topics.TopicWorkloadAssignments, start, err)
		return fmt.Errorf("read workload.assignments partition count: %w", err)
	}
	if err := validateWorkerCapacity(len(specs), partitions); err != nil {
		recordProduce(topics.TopicWorkloadAssignments, start, err)
		return err
	}
	msgs, err := buildWorkloadMessages(specs)
	if err != nil {
		recordProduce(topics.TopicWorkloadAssignments, start, err)
		return err
	}
	p.log.Info("publishing workload specs with explicit partition assignment",
		"worker_count", len(specs),
		"workload_partitions", partitions,
		"note", "bot-fleet replicas must be >= worker_count before fan-in completes (KEDA pre-scale)",
	)
	writeCtx, cancel := context.WithTimeout(ctx, writerTimeout)
	defer cancel()

	err = p.workloadWriter.WriteMessages(writeCtx, msgs...)
	recordProduce(topics.TopicWorkloadAssignments, start, err)
	return err
}

// buildWorkloadMessages performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func buildWorkloadMessages(specs []topics.WorkloadSpec) ([]kafka.Message, error) {
	msgs := make([]kafka.Message, 0, len(specs))
	for _, spec := range specs {
		payload, err := json.Marshal(spec)
		if err != nil {
			return nil, fmt.Errorf("marshal workload spec: %w", err)
		}
		msgs = append(msgs, kafka.Message{
			Key:        []byte(fmt.Sprintf("%s:%d", spec.SessionID, spec.WorkerIndex)),
			Value:      payload,
			WriterData: spec.WorkerIndex,
		})
	}
	return msgs, nil
}

// workloadPartitionCount applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (p *Producer) workloadPartitionCount(ctx context.Context) (int, error) {
	if p.workloadPartitions > 0 {
		return p.workloadPartitions, nil
	}
	client := &kafka.Client{Addr: p.workloadWriter.Addr, Timeout: writerTimeout}
	resp, err := client.Metadata(ctx, &kafka.MetadataRequest{Topics: []string{topics.TopicWorkloadAssignments}})
	if err != nil {
		return 0, err
	}
	for _, t := range resp.Topics {
		if t.Name != topics.TopicWorkloadAssignments {
			continue
		}
		if t.Error != nil {
			return 0, fmt.Errorf("topic metadata: %w", t.Error)
		}
		p.workloadPartitions = len(t.Partitions)
		return p.workloadPartitions, nil
	}
	return 0, fmt.Errorf("topic %s missing from metadata response", topics.TopicWorkloadAssignments)
}

// PublishBarrier applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
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

// PublishStatus applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
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

// parseBrokers performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
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

// recordProduce performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func recordProduce(topic string, start time.Time, err error) {
	result := "ok"
	if err != nil {
		result = "error"
	}
	labels := metrics.Labels("service", "bot-fleet-controller", "topic", topic, "result", result)
	metrics.Counter("kafka_messages_produced_total", "Kafka messages produced by topic and result.", labels, 1)
	metrics.Histogram("kafka_produce_duration_seconds", "Kafka produce duration in seconds.", labels, metrics.SinceSeconds(start))
}
