package pipeline

import (
	"testing"

	"github.com/iicpc/schemas/topics"
)

func sent(id, side, payload, ordType string, price, qty uint64) topics.OrderSentEvent {
	return topics.OrderSentEvent{
		SessionID: "S", OrderID: id, Side: side, PayloadType: payload, OrdType: ordType,
		Price: price, Qty: qty,
	}
}

func acked(id string, srcPort uint16, tcpSeq uint32, t3 uint64, exec string, fillQty, fillPrice uint64) topics.OrderAckedEvent {
	return topics.OrderAckedEvent{
		SessionID: "S", ContestantID: "c1", OrderID: id,
		SrcIP: 0x0a000001, SrcPort: srcPort, TCPSeq: tcpSeq,
		T3XDPIngressNS: t3, T7XDPEgressNS: t3 + 1000, PodServiceTimeNS: 1000,
		ExecType: exec, FillQty: fillQty, FillPrice: fillPrice,
	}
}

func TestEndToEndCleanSessionWithPhantomAndExcludedOrder(t *testing.T) {
	sents := []topics.OrderSentEvent{
		sent("B1", "BUY", "NEW", "LIMIT", 100, 10),
		sent("S1", "SELL", "NEW", "LIMIT", 100, 10),
		sent("NOACK", "BUY", "NEW", "LIMIT", 100, 5), // sent but never delivered (no acked)
	}
	// fill_price is fixed-point scaled by TelemetryPriceScale in production (eBPF
	// parses the contestant's decimal ×1e9); sent price 100 -> reported 100*scale.
	fp := 100 * topics.TelemetryPriceScale
	ackeds := []topics.OrderAckedEvent{
		// B1 arrives first (tcp_seq 1) on the flow, rests; S1 (tcp_seq 2) crosses it.
		acked("B1", 5, 1, 10, "2", 10, fp),
		acked("S1", 5, 2, 10, "2", 10, fp),
		acked("ghost", 5, 3, 10, "2", 5, fp), // never sent -> phantom
	}

	orders, phantoms := Assemble(sents, ackeds)
	if len(orders) != 2 {
		t.Fatalf("expected 2 assembled orders (NOACK excluded), got %d", len(orders))
	}
	if len(phantoms) != 1 || phantoms[0].OrderID != "ghost" {
		t.Fatalf("expected 1 phantom (ghost), got %+v", phantoms)
	}

	report, _, contestant := Run(sents, ackeds)
	if contestant != "c1" {
		t.Fatalf("contestant_id = %q, want c1", contestant)
	}
	if report.TotalFills != 3 || report.ValidFills != 2 || report.PhantomFills != 1 {
		t.Fatalf("expected 3 total / 2 valid / 1 phantom, got %+v", report)
	}
	if got := report.CorrectnessScore(); got < 0.66 || got > 0.67 {
		t.Fatalf("expected score ~0.667, got %v", got)
	}
}

// TestRunCountsTelemetryCompleteness pins the completeness counters the
// pipeline reports alongside the verdict: SentEvents = orders.sent events
// drained, AckedEvents = orders.acked events drained (post-dedup, the caller's
// slice), MatchedOrders = distinct orders present in BOTH streams (the
// replay's actual inputs). A sent order with no ack (NOACK) and an acked order
// never sent (ghost) must each count in their own stream but NOT in matched —
// the gap between SentEvents and MatchedOrders is exactly the signal
// score-computer's coverage gate consumes.
func TestRunCountsTelemetryCompleteness(t *testing.T) {
	fp := 100 * topics.TelemetryPriceScale
	sents := []topics.OrderSentEvent{
		sent("B1", "BUY", "NEW", "LIMIT", 100, 10),
		sent("S1", "SELL", "NEW", "LIMIT", 100, 10),
		sent("NOACK", "BUY", "NEW", "LIMIT", 100, 5), // lost/absent ack -> not matched
	}
	ackeds := []topics.OrderAckedEvent{
		acked("B1", 5, 1, 10, "0", 0, 0), // two responses for B1: one order, two acked events
		acked("B1", 5, 1, 10, "2", 10, fp),
		acked("S1", 5, 2, 10, "2", 10, fp),
		acked("ghost", 5, 3, 10, "2", 5, fp), // never sent -> counted as acked, not matched
	}

	_, counts, _ := Run(sents, ackeds)
	want := Counts{SentEvents: 3, AckedEvents: 4, MatchedOrders: 2}
	if counts != want {
		t.Fatalf("Run counts = %+v, want %+v", counts, want)
	}

	// Empty drain: all-zero counts (downstream treats that as "unknown", and a
	// session with zero telemetry has nothing to gate anyway).
	if _, counts, _ := Run(nil, nil); counts != (Counts{}) {
		t.Fatalf("Run(nil, nil) counts = %+v, want zero", counts)
	}
}

// TestRunScaledPriceDomain reproduces C1: orders.sent.price is a raw integer
// (the bot writes FIX tag 44=<int>) while orders.acked.fill_price is fixed-point
// scaled by TelemetryPriceScale (eBPF parses the contestant's decimal ×1e9, see
// schema annotation). A correct validator must compare both in one domain;
// otherwise every legitimate fill is flagged a price violation and the
// CorrectnessScore collapses to ~0, disqualifying every contestant at the 0.99 gate.
func TestRunScaledPriceDomain(t *testing.T) {
	const tick = uint64(10_000) // raw integer price the bot puts on the wire
	scaled := tick * topics.TelemetryPriceScale
	sents := []topics.OrderSentEvent{
		sent("MK", "SELL", "NEW", "LIMIT", tick, 10),
		sent("TK", "BUY", "NEW", "LIMIT", tick, 10),
	}
	ackeds := []topics.OrderAckedEvent{
		acked("MK", 5, 1, 10, "2", 10, scaled), // maker rests, then fills @ scaled price
		acked("TK", 5, 2, 20, "2", 10, scaled), // taker crosses, fills @ scaled price
	}
	report, _, _ := Run(sents, ackeds)
	if report.TotalFills != 2 {
		t.Fatalf("expected 2 reported fills, got %d (%+v)", report.TotalFills, report)
	}
	if report.ValidFills != 2 || report.PriceViolations != 0 {
		t.Fatalf("C1: legitimate fills at the scaled fill_price domain must be VALID; got %d valid, %d price-violations (%+v)",
			report.ValidFills, report.PriceViolations, report)
	}
}

// TestAssemblePrefersSentOrigOrderID reproduces H13: the reference engine must
// key cancel/replace off the bot-authoritative orders.sent.orig_order_id, not the
// contestant's echoed tag 41 in orders.acked, so a contestant can't steer the
// reference book by altering the cancel target.
func TestAssemblePrefersSentOrigOrderID(t *testing.T) {
	sents := []topics.OrderSentEvent{
		{SessionID: "S", OrderID: "C1", Side: "BUY", PayloadType: "CANCEL", OrdType: "LIMIT", OrigOrderID: "REAL-TARGET"},
	}
	a := acked("C1", 7, 1, 10, "0", 0, 0)
	a.OrigOrderID = "CONTESTANT-LIES" // contestant's echoed tag 41 disagrees with the bot
	orders, _ := Assemble(sents, []topics.OrderAckedEvent{a})
	if len(orders) != 1 {
		t.Fatalf("expected 1 order, got %d", len(orders))
	}
	if orders[0].OrigOrderID != "REAL-TARGET" {
		t.Errorf("H13: OrigOrderID = %q, want bot-authoritative REAL-TARGET", orders[0].OrigOrderID)
	}
}

func TestAssembleMapsKindAndFlow(t *testing.T) {
	sents := []topics.OrderSentEvent{
		sent("m1", "SELL", "NEW", "MARKET", 0, 7),
		sent("c1", "BUY", "CANCEL", "LIMIT", 0, 0),
	}
	ackeds := []topics.OrderAckedEvent{
		acked("m1", 9, 1, 100, "2", 7, 50),
		func() topics.OrderAckedEvent {
			a := acked("c1", 9, 2, 100, "0", 0, 0)
			a.OrigOrderID = "orig-1" // cancel carries tag 41
			return a
		}(),
	}
	orders, _ := Assemble(sents, ackeds)
	byID := map[string]int{}
	for i, o := range orders {
		byID[o.OrderID] = i
	}
	m1 := orders[byID["m1"]]
	if m1.Kind.String() != "NewMarket" {
		t.Errorf("m1 kind = %v, want NewMarket", m1.Kind)
	}
	if m1.Flow.SrcPort != 9 || m1.TCPSeq != 1 {
		t.Errorf("m1 flow/seq wrong: %+v", m1.Flow)
	}
	c1 := orders[byID["c1"]]
	if c1.Kind.String() != "Cancel" || c1.OrigOrderID != "orig-1" {
		t.Errorf("c1 cancel/orig wrong: kind=%v orig=%q", c1.Kind, c1.OrigOrderID)
	}
}
