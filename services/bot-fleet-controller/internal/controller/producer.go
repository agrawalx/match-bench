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

	// workloadPartitions caches the partition count of workload.assignments
	// after the first successful metadata lookup. Only PublishWorkloadSpec
	// reads/writes it, and that is called solely by the single Runner
	// goroutine (sessions are processed serially on the 1-replica
	// controller), so no lock is needed.
	workloadPartitions int
}

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
		// workload.assignments gets explicit per-worker partition assignment
		// (worker_index i → partition i%N) instead of key hashing: hash
		// collisions on session_id:worker_index keys can place two specs on
		// one partition, so one worker pod runs both serially (the second
		// misses the barrier) while another idles — silently degrading most
		// multi-worker (spike/ramp) runs. See workerIndexBalancer.
		workloadWriter: mk(topics.TopicWorkloadAssignments, &workerIndexBalancer{}),
		// Barrier and status hash the message Key (session_id) so a session
		// deterministically maps to one partition and its events stay
		// ordered. LeastBytes ignores the Key and scatters a session across
		// partitions — fine at 1 partition, but it breaks per-session ordering
		// (e.g. status walk applied out of order) the moment these topics are
		// created with >1 partition. Matches submission-api's keyed writer.
		barrierWriter: mk(topics.TopicBarrier, &kafka.Hash{}),
		statusWriter:  mk(topics.TopicBenchmarkStatusUpdated, &kafka.Hash{}),
		log:           log,
	}
}

// workerIndexBalancer routes each WorkloadSpec message to the partition
// matching its worker_index (i%numPartitions), read from the WriterData hint
// buildWorkloadMessages attaches. This is the 1:1 WorkloadSpec→pod mapping:
// with worker_count <= partitions (enforced by validateWorkerCapacity) every
// spec owns a distinct partition, so the bot-fleet consumer group hands at
// most one spec to each worker pod. Key hashing cannot guarantee that — two
// keys can hash to the same partition.
//
// A message without the hint (never produced by PublishWorkloadSpec; purely
// defensive) falls back to key hashing so it still routes deterministically.
type workerIndexBalancer struct {
	fallback kafka.Hash
}

func (b *workerIndexBalancer) Balance(msg kafka.Message, partitions ...int) int {
	idx, ok := msg.WriterData.(uint32)
	if !ok {
		return b.fallback.Balance(msg, partitions...)
	}
	return partitions[workerPartition(idx, len(partitions))]
}

// workerPartition is the pure partition assignment: worker_index i →
// partition i%numPartitions. The modulo is a safety net only — capacity
// validation rejects worker_count > partitions before anything is published,
// so in practice the mapping is the identity i → i.
func workerPartition(workerIndex uint32, numPartitions int) int {
	if numPartitions <= 0 {
		return 0
	}
	return int(workerIndex % uint32(numPartitions))
}

// validateWorkerCapacity enforces worker_count <= partition count on
// workload.assignments (24 in topic-init). More workers than partitions means
// the modulo wraps and two specs share a partition — the exact collision the
// explicit assignment exists to prevent — so the run must fail loudly instead
// of degrading silently. Raising the cap requires repartitioning the topic.
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
// session_id:worker_index and pinned to partition worker_index%N via the
// workload writer's workerIndexBalancer, so each spec owns one partition and
// the consumer group hands at most one spec to each worker pod.
//
// ============================ KEDA / SCALING ============================
// The 1:1 spec→pod mapping holds ONLY when, by the time fan-in starts:
//   - worker_count <= partition count (hard-enforced here; topic-init
//     provisions workload.assignments with 24 partitions), AND
//   - bot-fleet replicas >= worker_count, AND
//   - the worker group spreads consecutive partitions across distinct pods
//     (bot-fleet sets partition.assignment.strategy=roundrobin; the range
//     default would hand one pod a contiguous block of loaded partitions).
//
// KEDA scales bot-fleet on workload.assignments lag AFTER this publish, so
// the new pods join during the controller's ReadyDeadline fan-in window.
// For deterministic multi-worker runs, pre-scale the fleet (minReplicaCount
// >= the largest scenario's worker count) instead of relying on scale-up
// racing the fan-in. The log line below exists so an under-provisioned run
// is diagnosable from the controller's logs alone.
// ========================================================================
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

// buildWorkloadMessages assembles the workload.assignments messages: the Key
// stays session_id:worker_index (consumer-side identification and ordering),
// and WriterData carries the WorkerIndex hint workerIndexBalancer routes on.
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

// workloadPartitionCount returns the live partition count of
// workload.assignments, cached after the first successful lookup (the topic
// is provisioned once by topic-init; repartitioning implies a redeploy).
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
