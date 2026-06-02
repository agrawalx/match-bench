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
	ackeds := []topics.OrderAckedEvent{
		// B1 arrives first (tcp_seq 1) on the flow, rests; S1 (tcp_seq 2) crosses it.
		acked("B1", 5, 1, 10, "2", 10, 100),
		acked("S1", 5, 2, 10, "2", 10, 100),
		acked("ghost", 5, 3, 10, "2", 5, 100), // never sent -> phantom
	}

	orders, phantoms := Assemble(sents, ackeds)
	if len(orders) != 2 {
		t.Fatalf("expected 2 assembled orders (NOACK excluded), got %d", len(orders))
	}
	if len(phantoms) != 1 || phantoms[0].OrderID != "ghost" {
		t.Fatalf("expected 1 phantom (ghost), got %+v", phantoms)
	}

	report, contestant := Run(sents, ackeds)
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
