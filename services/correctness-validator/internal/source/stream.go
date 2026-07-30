// Streaming session source: bounded-memory replacement for DrainSession+pipeline.Run.
//
// It k-way merges every partition of orders.sent AND orders.acked by event time
// (sent → SendTSNS, ack → T3 ingress; the bot and eBPF nodes are NTP-synced within
// a few ms, so these are comparable within a small skew). A single consumer walks
// that merged stream, joins each order to its acks within a bounded join window, and
// feeds joined orders into the windowed Reorderer, which emits them in EXACTLY
// replay.Order's order (proven in reorder_test.go) to the validator.
//
// Memory is bounded everywhere: one batch buffered per partition reader, a join
// buffer holding ~JoinWindow worth of orders, the Reorderer's window, and the
// engine's live book. Nothing is O(session). Per-order build + EffectiveT3 promotion
// match the batch path exactly, so scores are identical.
package source

import (
	"context"
	"fmt"

	"github.com/iicpc/correctness-validator/internal/model"
	"github.com/iicpc/correctness-validator/internal/pipeline"
	"github.com/iicpc/libs/metrics"
	"github.com/iicpc/schemas/topics"
	"github.com/segmentio/kafka-go"
	"github.com/vmihailenco/msgpack/v5"
)

const (
	// DefaultReorderWindow caps the in-flight reorder buffer (orders).
	DefaultReorderWindow = 1 << 20
	// DefaultJoinWindowNS: an order's ack arrives within this of its send (NTP skew +
	// network RTT + jitter). 500ms is generous; orders are emitted once the merge's
	// event-time watermark passes their send time + this window.
	DefaultJoinWindowNS = uint64(500 * 1000 * 1000)
	partitionChanDepth  = 4
)

// StreamCounts mirrors pipeline.Counts for the store record.
type StreamCounts struct {
	SentEvents    uint64
	AckedEvents   uint64
	MatchedOrders uint64
}

// tsBatch is one decoded batch tagged with its lead event time, for the merge.
type tsBatch struct {
	ts    uint64 // sent: first event SendTSNS; ack: first event T3
	isAck bool
	sent  []topics.OrderSentEvent
	acks  []topics.OrderAckedEvent
}

// pendingOrder holds an order's sent + accumulated acks until it is emitted.
type pendingOrder struct {
	sent    topics.OrderSentEvent
	hasSent bool
	acks    []topics.OrderAckedEvent
	t3      uint64 // ack ingress time (shared across an order's acks); 0 until first ack
	sendTS  uint64 // sent send time; 0 until sent seen
}

// bandPartitionSet returns the set of partition IDs covered by band (mirroring
// topics.BandPartition's own base/wrap arithmetic), for numPartitions total
// partitions and bandWidth partitions per band. Used to restrict readers to
// only the session's exclusively-leased band instead of scanning every
// partition (see docs/multi-contestant-audit.md order-band section).
func bandPartitionSet(band uint32, numPartitions, bandWidth int32) map[int]struct{} {
	if numPartitions <= 1 {
		return map[int]struct{}{0: {}}
	}
	if bandWidth < 1 {
		bandWidth = 1
	}
	if bandWidth > numPartitions {
		bandWidth = numPartitions
	}
	base := int32(int64(band) * int64(bandWidth))
	set := make(map[int]struct{}, bandWidth)
	for i := int32(0); i < bandWidth; i++ {
		set[int((base+i)%numPartitions)] = struct{}{}
	}
	return set
}

// filterPartitions keeps only the partitions in allowed, preserving order. A
// nil allowed means "no restriction" (back-compat: OrderBandUnset).
func filterPartitions(parts []kafka.Partition, allowed map[int]struct{}) []kafka.Partition {
	if allowed == nil {
		return parts
	}
	out := make([]kafka.Partition, 0, len(allowed))
	for _, p := range parts {
		if _, ok := allowed[p.ID]; ok {
			out = append(out, p)
		}
	}
	return out
}

// StreamSession validates a session with bounded memory. apply is called with each
// order in EffectiveT3 order; addPhantom with each fill reported for an order_id that
// was never sent. orderBand/bandWidth restrict the partitions read to that
// session's exclusively-leased band (4x less broker read amplification than a
// full-topic scan); pass topics.OrderBandUnset for orderBand to fall back to
// reading every partition (back-compat with band-unaware sessions). The
// session-id filter in decodeBatch stays in effect regardless, as a guard
// against stale-tail cross-band leakage. Returns event counts and the
// contestant id.
func StreamSession(
	ctx context.Context,
	brokers []string,
	sessionID string,
	window int,
	orderBand uint32,
	bandWidth int32,
	apply func(*model.Order),
	addPhantom func(orderID string, qty uint64, price int64),
) (StreamCounts, string, error) {
	if window <= 0 {
		window = DefaultReorderWindow
	}
	var counts StreamCounts
	if len(brokers) == 0 {
		return counts, "", fmt.Errorf("no kafka brokers configured")
	}

	// Discover partitions of both topics.
	conn, err := kafka.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		return counts, "", fmt.Errorf("dial broker: %w", err)
	}
	sentParts, errS := conn.ReadPartitions(topics.TopicOrdersSent)
	ackParts, errA := conn.ReadPartitions(topics.TopicOrdersAcked)
	conn.Close()
	if errS != nil {
		return counts, "", fmt.Errorf("read partitions %s: %w", topics.TopicOrdersSent, errS)
	}
	if errA != nil {
		return counts, "", fmt.Errorf("read partitions %s: %w", topics.TopicOrdersAcked, errA)
	}

	if orderBand != topics.OrderBandUnset {
		sentParts = filterPartitions(sentParts, bandPartitionSet(orderBand, int32(len(sentParts)), bandWidth))
		ackParts = filterPartitions(ackParts, bandPartitionSet(orderBand, int32(len(ackParts)), bandWidth))
	}

	mctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Launch one reader per (topic, partition); each owns a bounded channel.
	type stream struct {
		ch   chan tsBatch
		head tsBatch
		live bool
		errp *error
	}
	var streams []*stream
	launch := func(topic string, partition int, isAck bool) {
		ch := make(chan tsBatch, partitionChanDepth)
		var rerr error
		s := &stream{ch: ch, errp: &rerr}
		streams = append(streams, s)
		go func() {
			defer close(ch)
			rerr = streamPartition(mctx, brokers, topic, partition, sessionID, isAck, ch)
		}()
	}
	for _, p := range sentParts {
		launch(topics.TopicOrdersSent, p.ID, false)
	}
	for _, p := range ackParts {
		launch(topics.TopicOrdersAcked, p.ID, true)
	}

	// Prime each stream's head.
	for _, s := range streams {
		if b, ok := <-s.ch; ok {
			s.head, s.live = b, true
		}
	}

	pending := make(map[string]*pendingOrder)
	order := make([]string, 0, 1024) // insertion order of pending ids, for FIFO emit
	contestant := ""
	r := NewReorderer(window, apply)
	joinWindow := DefaultJoinWindowNS

	// emitReady flushes orders whose send time is older than watermark-joinWindow
	// (all their acks have arrived) into the reorderer; if watermark==0 it flushes all.
	emitReady := func(watermark uint64, flushAll bool) {
		cut := 0
		for _, id := range order {
			po := pending[id]
			if po == nil {
				cut++
				continue // already emitted
			}
			ref := po.sendTS
			if ref == 0 {
				ref = po.t3
			}
			if !flushAll && ref+joinWindow > watermark {
				break // this and everything after it (FIFO by send time) is too recent
			}
			if po.hasSent && len(po.acks) > 0 {
				o := pipeline.AssembleOrder(po.sent, po.acks)
				if o != nil {
					counts.MatchedOrders++
					r.Push(o)
				}
			} else if !po.hasSent {
				for _, a := range po.acks { // ack with no sent → phantom fills
					if streamIsFill(a.ExecType, a.FillQty) {
						addPhantom(id, a.FillQty, int64(a.FillPrice))
					}
				}
			}
			delete(pending, id)
			cut++
		}
		if cut > 0 {
			order = order[cut:]
		}
	}

	// k-way merge by event time.
	for {
		// pick the live stream with the smallest head.ts
		min := -1
		for i, s := range streams {
			if s.live && (min < 0 || s.head.ts < streams[min].head.ts) {
				min = i
			}
		}
		if min < 0 {
			break // all streams drained
		}
		b := streams[min].head
		// process this batch
		if b.isAck {
			for _, a := range b.acks {
				counts.AckedEvents++
				if contestant == "" {
					contestant = a.ContestantID
				}
				po := pending[a.OrderID]
				if po == nil {
					po = &pendingOrder{}
					pending[a.OrderID] = po
					order = append(order, a.OrderID)
				}
				po.acks = append(po.acks, a)
				if po.t3 == 0 {
					po.t3 = a.T3XDPIngressNS
				}
			}
		} else {
			for _, s := range b.sent {
				counts.SentEvents++
				po := pending[s.OrderID]
				if po == nil {
					po = &pendingOrder{}
					pending[s.OrderID] = po
					order = append(order, s.OrderID)
				}
				po.sent, po.hasSent, po.sendTS = s, true, s.SendTSNS
			}
		}
		emitReady(b.ts, false)
		// refill head
		if nb, ok := <-streams[min].ch; ok {
			streams[min].head = nb
		} else {
			streams[min].live = false
		}
	}

	// drain: emit everything left, then flush the reorderer.
	emitReady(0, true)
	r.Flush()

	for _, s := range streams {
		if s.errp != nil && *s.errp != nil {
			return counts, contestant, fmt.Errorf("partition reader: %w", *s.errp)
		}
	}
	metrics.Histogram("validator_session_events_buffered",
		"Bounded in-flight orders per validated session (streaming).", nil, float64(window))
	return counts, contestant, nil
}

func streamIsFill(execType string, qty uint64) bool {
	return qty > 0 && (execType == "1" || execType == "2" || execType == "F")
}

// streamPartition reads [start,last) of one (topic,partition) for the session and
// sends each decoded batch (tagged with its lead event time) to ch.
func streamPartition(ctx context.Context, brokers []string, topic string, partition int, sessionID string, isAck bool, ch chan<- tsBatch) error {
	start, last, err := partitionOffsets(ctx, brokers[0], topic, partition, sessionID)
	if err != nil {
		return err
	}
	if start >= last {
		return nil
	}
	r := kafka.NewReader(kafka.ReaderConfig{Brokers: brokers, Topic: topic, Partition: partition, MinBytes: 1, MaxBytes: 10 << 20})
	defer r.Close()
	if err := r.SetOffset(start); err != nil {
		return fmt.Errorf("set offset %s/%d: %w", topic, partition, err)
	}
	for {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			return fmt.Errorf("read %s/%d: %w", topic, partition, err)
		}
		if b, ok := decodeBatch(m, sessionID, isAck); ok {
			select {
			case ch <- b:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		if m.Offset >= last-1 {
			break
		}
	}
	return nil
}

// decodeBatch unmarshals a Kafka message into a tsBatch for the session, or ok=false
// to skip (decode error, other session, or empty).
func decodeBatch(m kafka.Message, sessionID string, isAck bool) (tsBatch, bool) {
	if isAck {
		var b topics.OrderAckedBatch
		if err := msgpack.Unmarshal(m.Value, &b); err != nil {
			recordDecodeError(topics.TopicOrdersAcked, m, err)
			return tsBatch{}, false
		}
		if b.SessionID != sessionID || len(b.Events) == 0 {
			return tsBatch{}, false
		}
		return tsBatch{ts: b.Events[0].T3XDPIngressNS, isAck: true, acks: b.Events}, true
	}
	// orders.sent uses the POSITIONAL V2 envelope (Rust OrderSentBatchV2Ref) with
	// session_id/submission_id/worker_id hoisted out of the per-event payload. Decoding
	// it as the older named-map OrderSentBatch does not error — it silently yields a
	// garbage SessionID, so the filter below dropped every batch and this validator
	// reported sent=0 against a topic holding ~600k records. See
	// schemas/go/topics/wire_contract_test.go, which pins the layout against real
	// producer bytes.
	var b topics.OrderSentBatchV2
	if err := msgpack.Unmarshal(m.Value, &b); err != nil {
		recordDecodeError(topics.TopicOrdersSent, m, err)
		return tsBatch{}, false
	}
	if b.SessionID != sessionID || len(b.Events) == 0 {
		return tsBatch{}, false
	}
	events := b.IntoEvents()
	return tsBatch{ts: events[0].SendTSNS, isAck: false, sent: events}, true
}
