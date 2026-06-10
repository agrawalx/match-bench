// Package source implements drain behavior.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package source

import (
	"context"
	"encoding/hex"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/iicpc/libs/metrics"
	"github.com/iicpc/schemas/topics"
	"github.com/segmentio/kafka-go"
	"github.com/vmihailenco/msgpack/v5"
)

const startMargin = 60 * time.Second

// recordDecodeError performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func recordDecodeError(topic string, m kafka.Message, err error) {
	metrics.Counter("validator_validation_errors_total", "Correctness-validator validation errors by stage.", metrics.Labels("stage", "drain_decode"), 1)
	slog.Warn("drain: msgpack decode failed; skipping message", "topic", topic, "partition", m.Partition, "offset", m.Offset, "error", err)
}

// sentCollector groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type sentCollector struct {
	sessionID    string
	events       []topics.OrderSentEvent
	decodeErrors int
}

// handle applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (c *sentCollector) handle(m kafka.Message) {
	var b topics.OrderSentBatch
	if err := msgpack.Unmarshal(m.Value, &b); err != nil {
		c.decodeErrors++
		recordDecodeError(topics.TopicOrdersSent, m, err)
		return
	}
	if b.SessionID == c.sessionID {
		c.events = append(c.events, b.Events...)
	}
}

// ackKey groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type ackKey struct {
	orderID  string
	execType string
	t7NS     uint64
}

// ackedCollector groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type ackedCollector struct {
	sessionID    string
	seen         map[ackKey]struct{}
	events       []topics.OrderAckedEvent
	duplicates   int
	decodeErrors int
}

// newAckedCollector performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func newAckedCollector(sessionID string) *ackedCollector {
	return &ackedCollector{sessionID: sessionID, seen: make(map[ackKey]struct{})}
}

// handle applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (c *ackedCollector) handle(m kafka.Message) {
	var b topics.OrderAckedBatch
	if err := msgpack.Unmarshal(m.Value, &b); err != nil {
		c.decodeErrors++
		recordDecodeError(topics.TopicOrdersAcked, m, err)
		return
	}
	if b.SessionID != c.sessionID {
		return
	}
	for _, e := range b.Events {
		k := ackKey{orderID: e.OrderID, execType: e.ExecType, t7NS: e.T7XDPEgressNS}
		if _, dup := c.seen[k]; dup {
			c.duplicates++
			continue
		}
		c.seen[k] = struct{}{}
		c.events = append(c.events, e)
	}
}

// DrainSession performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func DrainSession(ctx context.Context, brokers []string, sessionID string) ([]topics.OrderSentEvent, []topics.OrderAckedEvent, error) {
	sc := &sentCollector{sessionID: sessionID}
	if err := drainTopic(ctx, brokers, topics.TopicOrdersSent, sessionID, sc.handle); err != nil {
		return nil, nil, fmt.Errorf("drain orders.sent: %w", err)
	}

	ac := newAckedCollector(sessionID)
	if err := drainTopic(ctx, brokers, topics.TopicOrdersAcked, sessionID, ac.handle); err != nil {
		return nil, nil, fmt.Errorf("drain orders.acked: %w", err)
	}
	if ac.duplicates > 0 {
		metrics.Counter("validator_events_drained_total", "Correctness-validator events drained from Kafka by topic.", metrics.Labels("topic", "orders_acked_duplicates"), float64(ac.duplicates))
		slog.Warn("drain: dropped duplicate orders.acked events", "session_id", sessionID, "duplicates", ac.duplicates)
	}

	return sc.events, ac.events, nil
}

// drainTopic performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func drainTopic(ctx context.Context, brokers []string, topic, sessionID string, handle func(kafka.Message)) error {
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
			handle(m)
			if m.Offset >= last-1 { // reached the snapshotted watermark
				break
			}
		}
		r.Close()
	}
	return nil
}

// partitionOffsets performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
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
	last, err := conn.ReadLastOffset()
	if err != nil {
		return 0, 0, fmt.Errorf("read last offset %s/%d: %w", topic, partition, err)
	}
	if start == kafka.FirstOffset {
		earliest, err := conn.ReadFirstOffset()
		if err != nil {
			return 0, 0, fmt.Errorf("read first offset %s/%d: %w", topic, partition, err)
		}
		return earliest, last, nil
	}
	return resolveStart(start, last), last, nil
}

// resolveStart performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func resolveStart(seek, last int64) int64 {
	if seek < 0 {
		return last
	}
	return seek
}

// startOffsetForSession performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func startOffsetForSession(sessionID string, lookup func(time.Time) (int64, error)) (int64, error) {
	start, ok := sessionStartFromID(sessionID)
	if !ok {
		return kafka.FirstOffset, nil
	}
	return lookup(start.Add(-startMargin))
}

// sessionStartFromID performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func sessionStartFromID(sessionID string) (time.Time, bool) {
	hexDigits := strings.ReplaceAll(sessionID, "-", "")
	if len(hexDigits) != 32 {
		return time.Time{}, false
	}
	raw, err := hex.DecodeString(hexDigits)
	if err != nil {
		return time.Time{}, false // not a hex UUID (e.g. synthetic "itest-..." id)
	}
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
