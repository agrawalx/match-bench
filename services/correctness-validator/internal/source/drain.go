// Package source drains a completed session's full order log from Kafka.
//
// On the completion trigger we snapshot each partition's high-watermark and read
// orders.sent + orders.acked up to that watermark, keeping only batches for the
// target session_id. Watermark-at-trigger is the completeness oracle (the
// controller publishes `completed` only after every producer acked), so reading
// to the watermark = reading every event. All partitions are read explicitly (we
// don't rely on kafka-go ↔ rdkafka partitioner parity).
//
// The START offset is bounded to the session's time window rather than offset 0.
// These topics accumulate every session's data, so draining one session from
// earliest would scan the WHOLE topic — the per-message msgpack churn over
// millions of messages × concurrently-validating sessions blows the heap
// (OOMKilled even at 3Gi). Since session_id is a UUIDv7 whose first 48 bits are
// the Unix-ms creation time, we derive the session start, subtract a safety
// margin, and use Kafka's time-based offset lookup (Conn.ReadOffset) to start
// each partition at the first message at-or-after that instant. All of the
// session's events are produced after session-start, so the result is identical
// — only the scan is bounded. Ids that aren't valid UUIDv7 (e.g. synthetic test
// ids) fall back to earliest, so we never silently lose events.
package source

import (
	"context"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/iicpc/schemas/topics"
	"github.com/segmentio/kafka-go"
	"github.com/vmihailenco/msgpack/v5"
)

// startMargin is subtracted from the session-start timestamp before the
// time-based offset lookup, guarding against clock skew between the session-id
// minter and Kafka brokers and against producers that ran slightly ahead of the
// id's timestamp. 60s is generous relative to any realistic skew while still
// bounding the scan to seconds-of-data instead of the whole topic.
const startMargin = 60 * time.Second

// DrainSession returns every orders.sent + orders.acked event for sessionID.
func DrainSession(ctx context.Context, brokers []string, sessionID string) ([]topics.OrderSentEvent, []topics.OrderAckedEvent, error) {
	var sents []topics.OrderSentEvent
	if err := drainTopic(ctx, brokers, topics.TopicOrdersSent, sessionID, func(v []byte) {
		var b topics.OrderSentBatch
		if msgpack.Unmarshal(v, &b) == nil && b.SessionID == sessionID {
			sents = append(sents, b.Events...)
		}
	}); err != nil {
		return nil, nil, fmt.Errorf("drain orders.sent: %w", err)
	}

	var ackeds []topics.OrderAckedEvent
	if err := drainTopic(ctx, brokers, topics.TopicOrdersAcked, sessionID, func(v []byte) {
		var b topics.OrderAckedBatch
		if msgpack.Unmarshal(v, &b) == nil && b.SessionID == sessionID {
			ackeds = append(ackeds, b.Events...)
		}
	}); err != nil {
		return nil, nil, fmt.Errorf("drain orders.acked: %w", err)
	}

	return sents, ackeds, nil
}

func drainTopic(ctx context.Context, brokers []string, topic, sessionID string, handle func([]byte)) error {
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
		start, last, err := partitionOffsets(ctx, brokers[0], topic, p.ID, sessionID)
		if err != nil {
			return err
		}
		if start >= last {
			continue // nothing in [start, watermark) — empty partition or no events in the session window
		}
		r := kafka.NewReader(kafka.ReaderConfig{
			Brokers:   brokers,
			Topic:     topic,
			Partition: p.ID,
			MinBytes:  1,
			MaxBytes:  10 << 20,
		})
		if err := r.SetOffset(start); err != nil {
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

// partitionOffsets returns (startOffset, highWatermark) for a partition, where
// startOffset is bounded to the session's time window (see package doc) rather
// than the partition's earliest offset. The high-watermark is the snapshotted
// completeness oracle; reading [start, watermark) captures every event for the
// session while scanning only the session's window of the topic.
func partitionOffsets(ctx context.Context, broker, topic string, partition int, sessionID string) (int64, int64, error) {
	conn, err := kafka.DialLeader(ctx, "tcp", broker, topic, partition)
	if err != nil {
		return 0, 0, fmt.Errorf("dial leader %s/%d: %w", topic, partition, err)
	}
	defer conn.Close()

	start, err := startOffsetForSession(sessionID, conn.ReadOffset)
	if err != nil {
		return 0, 0, fmt.Errorf("start offset %s/%d: %w", topic, partition, err)
	}
	if start == kafka.FirstOffset {
		// Non-UUIDv7 id: fall back to the partition's earliest offset so no event
		// is ever missed. ReadOffset(zero time) would mean "epoch", which Kafka
		// also resolves to earliest, but ReadFirstOffset is the explicit primitive.
		if start, err = conn.ReadFirstOffset(); err != nil {
			return 0, 0, fmt.Errorf("read first offset %s/%d: %w", topic, partition, err)
		}
	}
	last, err := conn.ReadLastOffset()
	if err != nil {
		return 0, 0, fmt.Errorf("read last offset %s/%d: %w", topic, partition, err)
	}
	return start, last, nil
}

// startOffsetForSession returns the offset to begin reading a partition from for
// sessionID. If sessionID is a UUIDv7, it resolves the offset of the first
// message produced at-or-after (session-start − startMargin) via the supplied
// time-based lookup (kafka.Conn.ReadOffset). Otherwise it returns
// kafka.FirstOffset, signalling the caller to read from earliest (no events lost).
//
// The lookup is injected so the bounding policy is unit-testable without a broker.
func startOffsetForSession(sessionID string, lookup func(time.Time) (int64, error)) (int64, error) {
	start, ok := sessionStartFromID(sessionID)
	if !ok {
		return kafka.FirstOffset, nil
	}
	return lookup(start.Add(-startMargin))
}

// sessionStartFromID extracts the creation time embedded in a UUIDv7 session id.
// Per RFC 9562 the first 48 bits of a v7 UUID are the Unix-millisecond timestamp.
// Returns (_, false) for ids that aren't well-formed UUIDv7 — including the
// synthetic "itest-..." ids used in tests — so callers fall back to earliest.
//
// We derive session-start from the id itself rather than reading the run row's
// created_at/started_at from Postgres: the id is already in hand at drain time,
// so this avoids a DB round-trip on the hot completion path while giving the same
// "recent" lower bound. A non-v7 id simply falls back to a full (earliest) scan.
func sessionStartFromID(sessionID string) (time.Time, bool) {
	hexDigits := strings.ReplaceAll(sessionID, "-", "")
	if len(hexDigits) != 32 {
		return time.Time{}, false
	}
	raw, err := hex.DecodeString(hexDigits)
	if err != nil {
		return time.Time{}, false // not a hex UUID (e.g. synthetic "itest-..." id)
	}
	// Version is the high nibble of byte 6; v7 carries the ms timestamp.
	if raw[6]>>4 != 7 {
		return time.Time{}, false
	}
	var ms int64
	for _, b := range raw[:6] { // first 48 bits, big-endian
		ms = ms<<8 | int64(b)
	}
	if ms <= 0 {
		return time.Time{}, false // implausible / zero timestamp — don't trust it
	}
	return time.UnixMilli(ms), true
}
