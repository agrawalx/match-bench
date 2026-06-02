// Package source drains a completed session's full order log from Kafka.
//
// On the completion trigger we snapshot each partition's high-watermark and read
// orders.sent + orders.acked from the earliest offset up to that watermark,
// keeping only batches for the target session_id. Watermark-at-trigger is the
// completeness oracle (the controller publishes `completed` only after every
// producer acked), so reading to the watermark = reading every event. All
// partitions are read explicitly (we don't rely on kafka-go ↔ rdkafka
// partitioner parity). Kafka's 1-day retention easily covers a post-run drain.
package source

import (
	"context"
	"fmt"

	"github.com/iicpc/schemas/topics"
	"github.com/segmentio/kafka-go"
	"github.com/vmihailenco/msgpack/v5"
)

// DrainSession returns every orders.sent + orders.acked event for sessionID.
func DrainSession(ctx context.Context, brokers []string, sessionID string) ([]topics.OrderSentEvent, []topics.OrderAckedEvent, error) {
	var sents []topics.OrderSentEvent
	if err := drainTopic(ctx, brokers, topics.TopicOrdersSent, func(v []byte) {
		var b topics.OrderSentBatch
		if msgpack.Unmarshal(v, &b) == nil && b.SessionID == sessionID {
			sents = append(sents, b.Events...)
		}
	}); err != nil {
		return nil, nil, fmt.Errorf("drain orders.sent: %w", err)
	}

	var ackeds []topics.OrderAckedEvent
	if err := drainTopic(ctx, brokers, topics.TopicOrdersAcked, func(v []byte) {
		var b topics.OrderAckedBatch
		if msgpack.Unmarshal(v, &b) == nil && b.SessionID == sessionID {
			ackeds = append(ackeds, b.Events...)
		}
	}); err != nil {
		return nil, nil, fmt.Errorf("drain orders.acked: %w", err)
	}

	return sents, ackeds, nil
}

func drainTopic(ctx context.Context, brokers []string, topic string, handle func([]byte)) error {
	if len(brokers) == 0 {
		return fmt.Errorf("no kafka brokers configured")
	}
	conn, err := kafka.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		return fmt.Errorf("dial broker: %w", err)
	}
	parts, err := conn.ReadPartitions(topic)
	conn.Close()
	if err != nil {
		return fmt.Errorf("read partitions for %s: %w", topic, err)
	}

	for _, p := range parts {
		first, last, err := partitionOffsets(ctx, brokers[0], topic, p.ID)
		if err != nil {
			return err
		}
		if first >= last {
			continue // empty partition — most are, since a session keys to one
		}
		r := kafka.NewReader(kafka.ReaderConfig{
			Brokers:   brokers,
			Topic:     topic,
			Partition: p.ID,
			MinBytes:  1,
			MaxBytes:  10 << 20,
		})
		if err := r.SetOffset(first); err != nil {
			r.Close()
			return fmt.Errorf("set offset %s/%d: %w", topic, p.ID, err)
		}
		for {
			m, err := r.ReadMessage(ctx)
			if err != nil {
				r.Close()
				return fmt.Errorf("read %s/%d: %w", topic, p.ID, err)
			}
			handle(m.Value)
			if m.Offset >= last-1 { // reached the snapshotted watermark
				break
			}
		}
		r.Close()
	}
	return nil
}

// partitionOffsets returns (firstOffset, highWatermark) for a partition.
func partitionOffsets(ctx context.Context, broker, topic string, partition int) (int64, int64, error) {
	conn, err := kafka.DialLeader(ctx, "tcp", broker, topic, partition)
	if err != nil {
		return 0, 0, fmt.Errorf("dial leader %s/%d: %w", topic, partition, err)
	}
	defer conn.Close()
	first, err := conn.ReadFirstOffset()
	if err != nil {
		return 0, 0, fmt.Errorf("read first offset %s/%d: %w", topic, partition, err)
	}
	last, err := conn.ReadLastOffset()
	if err != nil {
		return 0, 0, fmt.Errorf("read last offset %s/%d: %w", topic, partition, err)
	}
	return first, last, nil
}
