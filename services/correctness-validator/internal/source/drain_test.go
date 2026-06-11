// Package source defines tests for drain test.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
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

// TestSessionStartFromID performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestSessionStartFromID(t *testing.T) {
	const id = "019e8a61-64f4-7383-b63d-728af69ca072"
	const wantMS = int64(0x019e8a6164f4)

	got, ok := sessionStartFromID(id)
	if !ok {
		t.Fatalf("sessionStartFromID(%q) ok=false, want a valid UUIDv7 timestamp", id)
	}
	if got.UnixMilli() != wantMS {
		t.Fatalf("sessionStartFromID(%q) = %d ms, want %d ms", id, got.UnixMilli(), wantMS)
	}
}

// TestSessionStartFromIDFallback performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
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

// TestStartOffsetForSession performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestStartOffsetForSession(t *testing.T) {
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

// TestIntegration_DrainBoundedWindow performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestIntegration_DrainBoundedWindow(t *testing.T) {
	brokersCSV := strings.TrimSpace(os.Getenv("KAFKA_BROKERS"))
	if brokersCSV == "" {
		t.Skip("set KAFKA_BROKERS to run the bounded-drain integration test")
	}
	brokers := strings.Split(brokersCSV, ",")
	ctx := context.Background()

	old := newUUIDv7(time.Now().Add(-30 * time.Minute))
	writeSent(ctx, t, brokers, old, []topics.OrderSentEvent{
		{SessionID: old, OrderID: "OLD1", Price: 1, Qty: 1, Side: "BUY", PayloadType: "NEW", OrdType: "LIMIT"},
	})

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

// ackedMsg performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func ackedMsg(t *testing.T, b topics.OrderAckedBatch) kafka.Message {
	t.Helper()
	payload, err := msgpack.Marshal(b)
	if err != nil {
		t.Fatalf("msgpack marshal: %v", err)
	}
	return kafka.Message{Value: payload}
}

// TestAckedCollectorDedup performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestAckedCollectorDedup(t *testing.T) {
	const sid = "sess-dedup"
	fill := topics.OrderAckedEvent{SessionID: sid, OrderID: "A", ExecType: "2", FillQty: 5, FillPrice: 100, T7XDPEgressNS: 1000}
	otherT7 := fill
	otherT7.T7XDPEgressNS = 2000
	otherExec := fill
	otherExec.ExecType = "1"
	otherOrder := fill
	otherOrder.OrderID = "B"

	c := newAckedCollector(sid)
	c.handle(ackedMsg(t, topics.OrderAckedBatch{SessionID: sid, ContestantID: "team-dedup", Events: []topics.OrderAckedEvent{fill, otherT7}}))
	c.handle(ackedMsg(t, topics.OrderAckedBatch{SessionID: sid, ContestantID: "team-dedup", Events: []topics.OrderAckedEvent{fill, otherT7}}))
	c.handle(ackedMsg(t, topics.OrderAckedBatch{SessionID: sid, ContestantID: "team-dedup", Events: []topics.OrderAckedEvent{otherExec, otherOrder}}))
	c.handle(ackedMsg(t, topics.OrderAckedBatch{SessionID: "other-session", Events: []topics.OrderAckedEvent{fill}}))

	if len(c.events) != 4 {
		t.Fatalf("collected %d events, want 4 (dupes dropped, near-dupes kept): %+v", len(c.events), c.events)
	}
	want := []topics.OrderAckedEvent{fill, otherT7, otherExec, otherOrder}
	for i, e := range c.events {
		if e != want[i] {
			t.Errorf("events[%d] = %+v, want %+v (arrival order must be preserved)", i, e, want[i])
		}
	}
	if c.duplicates != 2 {
		t.Errorf("duplicates = %d, want 2", c.duplicates)
	}
}

// TestCollectorsCountDecodeErrors performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestCollectorsCountDecodeErrors(t *testing.T) {
	const sid = "sess-decode"
	garbage := kafka.Message{Partition: 3, Offset: 42, Value: []byte{0xc1}}

	sc := &sentCollector{sessionID: sid}
	sc.handle(garbage)
	sentPayload, err := msgpack.Marshal(topics.OrderSentBatch{
		SessionID: sid,
		Events:    []topics.OrderSentEvent{{SessionID: sid, OrderID: "A", Price: 100, Qty: 10, Side: "BUY", PayloadType: "NEW", OrdType: "LIMIT"}},
	})
	if err != nil {
		t.Fatalf("msgpack marshal: %v", err)
	}
	sc.handle(kafka.Message{Value: sentPayload})
	if sc.decodeErrors != 1 {
		t.Errorf("sentCollector.decodeErrors = %d, want 1", sc.decodeErrors)
	}
	if len(sc.events) != 1 {
		t.Errorf("sentCollector kept %d events, want 1 (decode failure must not sink the drain)", len(sc.events))
	}

	ac := newAckedCollector(sid)
	ac.handle(garbage)
	ac.handle(ackedMsg(t, topics.OrderAckedBatch{SessionID: sid, Events: []topics.OrderAckedEvent{
		{SessionID: sid, OrderID: "A", ExecType: "0", T7XDPEgressNS: 1500},
	}}))
	if ac.decodeErrors != 1 {
		t.Errorf("ackedCollector.decodeErrors = %d, want 1", ac.decodeErrors)
	}
	if len(ac.events) != 1 {
		t.Errorf("ackedCollector kept %d events, want 1 (decode failure must not sink the drain)", len(ac.events))
	}
}

// newUUIDv7 performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func newUUIDv7(at time.Time) string {
	ms := uint64(at.UnixMilli())
	return fmt.Sprintf("%012x-%04x-7%03x-8%03x-%012x",
		ms&0xffffffffffff, 0x64f4, 0x383, 0x63d, uint64(0x728af69ca072))
}

// writeSent performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
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

// TestResolveStart performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestResolveStart(t *testing.T) {
	if got := resolveStart(-1, 591); got != 591 {
		t.Fatalf("seek=-1 (no session events) must map to last=591, got %d", got)
	}
	if got := resolveStart(0, 297); got != 0 {
		t.Fatalf("seek=0 must be honored, got %d", got)
	}
	if got := resolveStart(42, 1197); got != 42 {
		t.Fatalf("seek=42 must be honored, got %d", got)
	}
}
