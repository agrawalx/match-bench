// Package pipeline defines tests for pipeline test.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package pipeline

import (
	"testing"

	"github.com/iicpc/schemas/topics"
)

// sent performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func sent(id, side, payload, ordType string, price, qty uint64) topics.OrderSentEvent {
	return topics.OrderSentEvent{
		SessionID: "S", OrderID: id, Side: side, PayloadType: payload, OrdType: ordType,
		Price: price, Qty: qty,
	}
}

// acked performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func acked(id string, srcPort uint16, tcpSeq uint32, t3 uint64, exec string, fillQty, fillPrice uint64) topics.OrderAckedEvent {
	return topics.OrderAckedEvent{
		SessionID: "S", ContestantID: "c1", OrderID: id,
		SrcIP: 0x0a000001, SrcPort: srcPort, TCPSeq: tcpSeq,
		T3XDPIngressNS: t3, T7XDPEgressNS: t3 + 1000, PodServiceTimeNS: 1000,
		ExecType: exec, FillQty: fillQty, FillPrice: fillPrice,
	}
}

// TestEndToEndCleanSessionWithPhantomAndExcludedOrder performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestEndToEndCleanSessionWithPhantomAndExcludedOrder(t *testing.T) {
	sents := []topics.OrderSentEvent{
		sent("B1", "BUY", "NEW", "LIMIT", 100, 10),
		sent("S1", "SELL", "NEW", "LIMIT", 100, 10),
		sent("NOACK", "BUY", "NEW", "LIMIT", 100, 5),
	}
	fp := 100 * topics.TelemetryPriceScale
	ackeds := []topics.OrderAckedEvent{
		acked("B1", 5, 1, 10, "2", 10, fp),
		acked("S1", 5, 2, 10, "2", 10, fp),
		acked("ghost", 5, 3, 10, "2", 5, fp),
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
	// Phantom-fill is no longer part of the score (docs/multi-contestant-audit.md §5):
	// a fabricated fill is already surfaced by PhantomFills/unmatched_responses, so
	// the 2 valid fills out of the 2 legitimately-sent orders score 1.0, not 2/3.
	if got := report.CorrectnessScore(); got != 1.0 {
		t.Fatalf("expected score 1.0 (phantom excluded from scoring), got %v", got)
	}
}

// TestRunCountsTelemetryCompleteness performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestRunCountsTelemetryCompleteness(t *testing.T) {
	fp := 100 * topics.TelemetryPriceScale
	sents := []topics.OrderSentEvent{
		sent("B1", "BUY", "NEW", "LIMIT", 100, 10),
		sent("S1", "SELL", "NEW", "LIMIT", 100, 10),
		sent("NOACK", "BUY", "NEW", "LIMIT", 100, 5),
	}
	ackeds := []topics.OrderAckedEvent{
		acked("B1", 5, 1, 10, "0", 0, 0),
		acked("B1", 5, 1, 10, "2", 10, fp),
		acked("S1", 5, 2, 10, "2", 10, fp),
		acked("ghost", 5, 3, 10, "2", 5, fp),
	}

	_, counts, _ := Run(sents, ackeds)
	want := Counts{SentEvents: 3, AckedEvents: 4, MatchedOrders: 2}
	if counts != want {
		t.Fatalf("Run counts = %+v, want %+v", counts, want)
	}

	if _, counts, _ := Run(nil, nil); counts != (Counts{}) {
		t.Fatalf("Run(nil, nil) counts = %+v, want zero", counts)
	}
}

// TestRunScaledPriceDomain performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestRunScaledPriceDomain(t *testing.T) {
	const tick = uint64(10_000)
	scaled := tick * topics.TelemetryPriceScale
	sents := []topics.OrderSentEvent{
		sent("MK", "SELL", "NEW", "LIMIT", tick, 10),
		sent("TK", "BUY", "NEW", "LIMIT", tick, 10),
	}
	ackeds := []topics.OrderAckedEvent{
		acked("MK", 5, 1, 10, "2", 10, scaled),
		acked("TK", 5, 2, 20, "2", 10, scaled),
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

// TestAssemblePrefersSentOrigOrderID performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestAssemblePrefersSentOrigOrderID(t *testing.T) {
	sents := []topics.OrderSentEvent{
		{SessionID: "S", OrderID: "C1", Side: "BUY", PayloadType: "CANCEL", OrdType: "LIMIT", OrigOrderID: "REAL-TARGET"},
	}
	a := acked("C1", 7, 1, 10, "0", 0, 0)
	a.OrigOrderID = "CONTESTANT-LIES"
	orders, _ := Assemble(sents, []topics.OrderAckedEvent{a})
	if len(orders) != 1 {
		t.Fatalf("expected 1 order, got %d", len(orders))
	}
	if orders[0].OrigOrderID != "REAL-TARGET" {
		t.Errorf("H13: OrigOrderID = %q, want bot-authoritative REAL-TARGET", orders[0].OrigOrderID)
	}
}

// TestAssembleMapsKindAndFlow performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestAssembleMapsKindAndFlow(t *testing.T) {
	sents := []topics.OrderSentEvent{
		sent("m1", "SELL", "NEW", "MARKET", 0, 7),
		sent("c1", "BUY", "CANCEL", "LIMIT", 0, 0),
	}
	ackeds := []topics.OrderAckedEvent{
		acked("m1", 9, 1, 100, "2", 7, 50),
		func() topics.OrderAckedEvent {
			a := acked("c1", 9, 2, 100, "0", 0, 0)
			a.OrigOrderID = "orig-1"
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
