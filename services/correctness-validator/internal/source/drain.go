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
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/iicpc/libs/metrics"
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

// recordDecodeError surfaces a message that failed msgpack decode during the
// drain: counted per stage and logged with its partition/offset so the poison
// message can be located, then skipped — one undecodable message must not sink
// (or silently distort) the whole session's validation.
func recordDecodeError(topic string, m kafka.Message, err error) {
	metrics.Counter("validator_validation_errors_total", "Correctness-validator validation errors by stage.", metrics.Labels("stage", "drain_decode"), 1)
	slog.Warn("drain: msgpack decode failed; skipping message", "topic", topic, "partition", m.Partition, "offset", m.Offset, "error", err)
}

// sentCollector accumulates one session's orders.sent events off the drained
// messages, counting (and skipping) msgpack decode failures.
type sentCollector struct {
	sessionID    string
	events       []topics.OrderSentEvent
	decodeErrors int
}

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

// ackKey identifies one orders.acked response for dedup:
// (order_id, exec_type, t7_xdp_egress_ns). The producers are at-least-once, so
// the same event can land on the topic twice; t7 (per-response-packet XDP egress
// timestamp, ns) distinguishes genuine repeat responses — e.g. two partial fills
// with the same exec_type — from redeliveries of the same one.
type ackKey struct {
	orderID  string
	execType string
	t7NS     uint64
}

// ackedCollector accumulates one session's orders.acked events, dropping
// redelivered duplicates by ackKey (a duplicated fill would inflate the
// reported cumulative quantity into a false overfill/phantom violation) and
// counting msgpack decode failures.
type ackedCollector struct {
	sessionID    string
	seen         map[ackKey]struct{}
	events       []topics.OrderAckedEvent
	duplicates   int
	decodeErrors int
}

func newAckedCollector(sessionID string) *ackedCollector {
	return &ackedCollector{sessionID: sessionID, seen: make(map[ackKey]struct{})}
}

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

// DrainSession returns every orders.sent + orders.acked event for sessionID,
// with at-least-once duplicates of acked events removed before assembly.
func DrainSession(ctx context.Context, brokers []string, sessionID string) ([]topics.OrderSentEvent, []topics.OrderAckedEvent, error) {
	// orders.sent and orders.acked are independent topics drained into separate
	// collectors, so drain them concurrently — halving wall-clock, which matters
	// because each topic's drain is already a multi-partition fan-out bounded by
	// the validation deadline.
	sc := &sentCollector{sessionID: sessionID}
	ac := newAckedCollector(sessionID)
	var sentErr, ackedErr error
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); sentErr = drainTopic(ctx, brokers, topics.TopicOrdersSent, sessionID, sc.handle) }()
	go func() { defer wg.Done(); ackedErr = drainTopic(ctx, brokers, topics.TopicOrdersAcked, sessionID, ac.handle) }()
	wg.Wait()
	if sentErr != nil {
		return nil, nil, fmt.Errorf("drain orders.sent: %w", sentErr)
	}
	if ackedErr != nil {
		return nil, nil, fmt.Errorf("drain orders.acked: %w", ackedErr)
	}
	if ac.duplicates > 0 {
		// Counted alongside the drained-events metric so total-seen = drained +
		// duplicates; dropped here so they can never inflate fills downstream.
		metrics.Counter("validator_events_drained_total", "Correctness-validator events drained from Kafka by topic.", metrics.Labels("topic", "orders_acked_duplicates"), float64(ac.duplicates))
		slog.Warn("drain: dropped duplicate orders.acked events", "session_id", sessionID, "duplicates", ac.duplicates)
	}

	return sc.events, ac.events, nil
}

// drainPartitionConcurrency bounds how many partition readers run at once. Each
// kafka-go partition Reader pays a fixed connection/initial-fetch cost (~seconds)
// independent of how few messages it returns, so draining the topic's partitions
// sequentially makes the whole drain scale with partition count — on a
// co-partitioned topic (orders.sent/acked are sharded across many partitions by
// order_id) that serial cost alone blew the validation deadline. Reading the
// partitions concurrently collapses it to ~one reader's cost. The cap keeps the
// fan-out (sockets + buffered batches) bounded so a high-partition topic can't
// exhaust fds or memory.
const drainPartitionConcurrency = 12

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

	// Cancel sibling readers as soon as one fails so a single partition error
	// doesn't leave the rest draining to the deadline.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// handle mutates shared collector state and is not safe for concurrent use;
	// serialize the (cheap) per-message append behind a mutex while the (slow)
	// network reads run in parallel.
	var mu sync.Mutex
	guarded := func(m kafka.Message) {
		mu.Lock()
		handle(m)
		mu.Unlock()
	}

	sem := make(chan struct{}, drainPartitionConcurrency)
	var wg sync.WaitGroup
	var errOnce sync.Once
	var firstErr error
	fail := func(err error) {
		errOnce.Do(func() {
			firstErr = err
			cancel()
		})
	}

	for _, p := range parts {
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
		}
		if ctx.Err() != nil {
			break
		}
		wg.Add(1)
		go func(partition int) {
			defer wg.Done()
			defer func() { <-sem }()
			if err := drainPartition(ctx, brokers, topic, partition, sessionID, guarded); err != nil {
				fail(err)
			}
		}(p.ID)
	}
	wg.Wait()
	if firstErr != nil {
		return firstErr
	}
	return ctx.Err()
}

// drainPartition reads one partition's [start, watermark) window and feeds every
// message to handle. handle must be safe to call from this goroutine (the caller
// serializes it); the network read itself is the part that runs concurrently.
func drainPartition(ctx context.Context, brokers []string, topic string, partition int, sessionID string, handle func(kafka.Message)) error {
	start, last, err := partitionOffsets(ctx, brokers[0], topic, partition, sessionID)
	if err != nil {
		return err
	}
	if start >= last {
		return nil // nothing in [start, watermark) — empty partition or no events in the session window
	}
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   brokers,
		Topic:     topic,
		Partition: partition,
		MinBytes:  1,
		MaxBytes:  10 << 20,
	})
	defer r.Close()
	if err := r.SetOffset(start); err != nil {
		return fmt.Errorf("set offset %s/%d: %w", topic, partition, err)
	}
	for {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			return fmt.Errorf("read %s/%d: %w", topic, partition, err)
		}
		handle(m)
		if m.Offset >= last-1 { // reached the snapshotted watermark
			break
		}
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
	last, err := conn.ReadLastOffset()
	if err != nil {
		return 0, 0, fmt.Errorf("read last offset %s/%d: %w", topic, partition, err)
	}
	if start == kafka.FirstOffset {
		// Non-UUIDv7 id: fall back to the partition's earliest offset so no event
		// is ever missed. ReadOffset(zero time) would mean "epoch", which Kafka
		// also resolves to earliest, but ReadFirstOffset is the explicit primitive.
		earliest, err := conn.ReadFirstOffset()
		if err != nil {
			return 0, 0, fmt.Errorf("read first offset %s/%d: %w", topic, partition, err)
		}
		return earliest, last, nil
	}
	return resolveStart(start, last), last, nil
}

// resolveStart turns the raw ReadOffset (time-lookup) result into a concrete
// start offset for the [start,last) scan. Kafka's ListOffsets returns -1 when no
// message in the partition has a timestamp at-or-after the requested time — i.e.
// the session produced nothing in this partition (it holds only older runs' data
// on a shared, retained topic). That must map to an EMPTY range (start=last), not
// fall through to SetOffset(-1)=LastOffset, which made the reader block on the
// high watermark until the validation deadline and drain zero events for the
// whole session (spurious timeout / zero-correctness verdict).
func resolveStart(seek, last int64) int64 {
	if seek < 0 {
		return last
	}
	return seek
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
