package source

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/iicpc/schemas/topics"
	"github.com/segmentio/kafka-go"
	"github.com/vmihailenco/msgpack/v5"
)

// TestSessionStartFromID checks the UUIDv7 timestamp extraction: the first 48
// bits of a v7 id are the Unix-millisecond creation time, which we use as the
// session's start to bound the Kafka scan.
func TestSessionStartFromID(t *testing.T) {
	// 019e8a61-64f4-... → first 48 bits 0x019e8a6164f4 = 1780438099188 ms.
	const id = "019e8a61-64f4-7383-b63d-728af69ca072"
	const wantMS = int64(0x019e8a6164f4) // 1780438099188

	got, ok := sessionStartFromID(id)
	if !ok {
		t.Fatalf("sessionStartFromID(%q) ok=false, want a valid UUIDv7 timestamp", id)
	}
	if got.UnixMilli() != wantMS {
		t.Fatalf("sessionStartFromID(%q) = %d ms, want %d ms", id, got.UnixMilli(), wantMS)
	}
}

// TestSessionStartFromIDFallback verifies that ids which aren't valid UUIDv7 fall
// back (ok=false) so the caller reads from earliest and never loses events.
func TestSessionStartFromIDFallback(t *testing.T) {
	cases := []struct {
		name string
		id   string
	}{
		{"synthetic test id", "itest-1780438099188"},
		{"empty", ""},
		{"too short", "019e8a61"},
		{"not hex", "zzzzzzzz-64f4-7383-b63d-728af69ca072"},
		{"v4 (version nibble != 7)", "019e8a61-64f4-4383-b63d-728af69ca072"},
		{"zero timestamp", "00000000-0000-7000-8000-000000000000"},
		{"no dashes, wrong length", "019e8a6164f47383b63d728af69ca0"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, ok := sessionStartFromID(c.id); ok {
				t.Errorf("sessionStartFromID(%q) ok=true, want false (must fall back to earliest)", c.id)
			}
		})
	}
}

// TestStartOffsetForSession is the unit-level check of the bounding policy with a
// fake offset lookup: given a session-start time, the start offset is whatever
// ReadOffset(session-start − margin) returns, and we never read past the
// snapshotted high-watermark. A non-UUIDv7 id falls back to kafka.FirstOffset.
func TestStartOffsetForSession(t *testing.T) {
	// Fake: messages were produced at ms 1000..2000; the broker would return the
	// offset of the first message at-or-after the requested time. We assert the
	// requested timestamp is start − margin, and pass back a canned offset.
	const validID = "019e8a61-64f4-7383-b63d-728af69ca072"
	startMS := int64(0x019e8a6164f4)

	var sawRequest time.Time
	lookup := func(at time.Time) (int64, error) {
		sawRequest = at
		return 4242, nil
	}

	got, err := startOffsetForSession(validID, lookup)
	if err != nil {
		t.Fatalf("startOffsetForSession: %v", err)
	}
	if got != 4242 {
		t.Fatalf("start offset = %d, want 4242 (the time-lookup result)", got)
	}
	wantReq := time.UnixMilli(startMS).Add(-startMargin)
	if !sawRequest.Equal(wantReq) {
		t.Fatalf("time lookup requested %v, want %v (session-start − %v margin)", sawRequest, wantReq, startMargin)
	}

	// Non-UUIDv7 id: must not call the broker; fall back to FirstOffset (earliest).
	called := false
	off, err := startOffsetForSession("itest-123", func(time.Time) (int64, error) {
		called = true
		return 0, nil
	})
	if err != nil {
		t.Fatalf("startOffsetForSession fallback: %v", err)
	}
	if called {
		t.Errorf("non-UUIDv7 id triggered a time lookup; want fallback without a broker round-trip")
	}
	if off != kafka.FirstOffset {
		t.Errorf("fallback start offset = %d, want kafka.FirstOffset (%d)", off, kafka.FirstOffset)
	}
}

// TestIntegration_DrainBoundedWindow proves the bounded drain still returns every
// event for a UUIDv7 session even when the topic also holds OLDER data for other
// sessions: the time-based start offset must sit at/before the session's events.
//
// Env-gated: needs a live broker. Mirrors main_integration_test.go style.
//
//	docker compose up -d
//	KAFKA_BROKERS=localhost:9092 go test ./internal/source/... -run Integration -v
func TestIntegration_DrainBoundedWindow(t *testing.T) {
	brokersCSV := strings.TrimSpace(os.Getenv("KAFKA_BROKERS"))
	if brokersCSV == "" {
		t.Skip("set KAFKA_BROKERS to run the bounded-drain integration test")
	}
	brokers := strings.Split(brokersCSV, ",")
	ctx := context.Background()

	// Old noise for an unrelated session (produced "in the past" relative to ours).
	old := newUUIDv7(time.Now().Add(-30 * time.Minute))
	writeSent(ctx, t, brokers, old, []topics.OrderSentEvent{
		{SessionID: old, OrderID: "OLD1", Price: 1, Qty: 1, Side: "BUY", PayloadType: "NEW", OrdType: "LIMIT"},
	})

	// Our session: its UUIDv7 timestamp is ~now, so the bounded scan starts late.
	sid := newUUIDv7(time.Now())
	want := []topics.OrderSentEvent{
		{SessionID: sid, OrderID: "A", Price: 100, Qty: 10, Side: "BUY", PayloadType: "NEW", OrdType: "LIMIT"},
		{SessionID: sid, OrderID: "B", Price: 101, Qty: 5, Side: "SELL", PayloadType: "NEW", OrdType: "LIMIT"},
	}
	writeSent(ctx, t, brokers, sid, want)

	sents, _, err := DrainSession(ctx, brokers, sid)
	if err != nil {
		t.Fatalf("DrainSession: %v", err)
	}
	if len(sents) != len(want) {
		t.Fatalf("DrainSession returned %d sent events, want %d: %+v", len(sents), len(want), sents)
	}
	gotIDs := map[string]bool{}
	for _, e := range sents {
		if e.SessionID != sid {
			t.Errorf("leaked event from another session: %+v", e)
		}
		gotIDs[e.OrderID] = true
	}
	if !gotIDs["A"] || !gotIDs["B"] {
		t.Errorf("missing events; got order ids %v, want A and B", gotIDs)
	}
}

// newUUIDv7 builds a minimal RFC-9562 UUIDv7 string with the given creation time
// in its first 48 bits (the rest is deterministic filler — enough for the parser).
func newUUIDv7(at time.Time) string {
	ms := uint64(at.UnixMilli())
	// 48-bit ms timestamp, then version 7, then variant bits.
	return fmt.Sprintf("%012x-%04x-7%03x-8%03x-%012x",
		ms&0xffffffffffff, 0x64f4, 0x383, 0x63d, uint64(0x728af69ca072))
}

func writeSent(ctx context.Context, t *testing.T, brokers []string, sid string, events []topics.OrderSentEvent) {
	t.Helper()
	batch := topics.OrderSentBatch{SessionID: sid, WorkerID: "w0", Events: events}
	payload, err := msgpack.Marshal(batch)
	if err != nil {
		t.Fatalf("msgpack marshal: %v", err)
	}
	w := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topics.TopicOrdersSent,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireAll,
	}
	defer w.Close()
	if err := w.WriteMessages(ctx, kafka.Message{Key: []byte(sid), Value: payload}); err != nil {
		t.Fatalf("write orders.sent: %v", err)
	}
}
