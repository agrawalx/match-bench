// Package replay defines tests for order test.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package replay

import (
	"testing"

	"github.com/iicpc/correctness-validator/internal/model"
)

// ids performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func ids(orders []*model.Order) []string {
	out := make([]string, len(orders))
	for i, o := range orders {
		out[i] = o.OrderID
	}
	return out
}

// eq performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func eq(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("order mismatch:\n got=%v\nwant=%v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("order mismatch at %d:\n got=%v\nwant=%v", i, got, want)
		}
	}
}

// TestEffectiveT3HOLWorkedExample performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestEffectiveT3HOLWorkedExample(t *testing.T) {
	fa := model.Flow{SrcIP: 1, SrcPort: 1}
	fb := model.Flow{SrcIP: 2, SrcPort: 2}
	orders := []*model.Order{
		{OrderID: "A1", Flow: fa, TCPSeq: 100, T3Ns: 50},
		{OrderID: "A2", Flow: fa, TCPSeq: 200, T3Ns: 10},
		{OrderID: "A3", Flow: fa, TCPSeq: 300, T3Ns: 15},
		{OrderID: "B1", Flow: fb, TCPSeq: 100, T3Ns: 12},
		{OrderID: "B2", Flow: fb, TCPSeq: 200, T3Ns: 25},
	}
	out := Order(orders)

	want := map[string]uint64{"A1": 50, "A2": 50, "A3": 50, "B1": 12, "B2": 25}
	for _, o := range orders {
		if o.EffectiveT3 != want[o.OrderID] {
			t.Errorf("%s effective_t3 = %d, want %d", o.OrderID, o.EffectiveT3, want[o.OrderID])
		}
	}
	eq(t, ids(out), []string{"B1", "B2", "A1", "A2", "A3"})
}

// TestInOrderFlowHasNoPromotion performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestInOrderFlowHasNoPromotion(t *testing.T) {
	f := model.Flow{SrcIP: 1, SrcPort: 1}
	orders := []*model.Order{
		{OrderID: "x1", Flow: f, TCPSeq: 10, T3Ns: 100},
		{OrderID: "x2", Flow: f, TCPSeq: 20, T3Ns: 200},
		{OrderID: "x3", Flow: f, TCPSeq: 30, T3Ns: 300},
	}
	out := Order(orders)
	eq(t, ids(out), []string{"x1", "x2", "x3"})
	if orders[0].EffectiveT3 != 100 || orders[1].EffectiveT3 != 200 || orders[2].EffectiveT3 != 300 {
		t.Errorf("expected no promotion, got %d %d %d", orders[0].EffectiveT3, orders[1].EffectiveT3, orders[2].EffectiveT3)
	}
}

// TestFullyReversedFlowCascadesToFirst performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestFullyReversedFlowCascadesToFirst(t *testing.T) {
	f := model.Flow{SrcIP: 1, SrcPort: 1}
	orders := []*model.Order{
		{OrderID: "r1", Flow: f, TCPSeq: 10, T3Ns: 300},
		{OrderID: "r2", Flow: f, TCPSeq: 20, T3Ns: 200},
		{OrderID: "r3", Flow: f, TCPSeq: 30, T3Ns: 100},
	}
	Order(orders)
	for _, o := range orders {
		if o.EffectiveT3 != 300 {
			t.Errorf("%s effective_t3 = %d, want 300 (cascaded)", o.OrderID, o.EffectiveT3)
		}
	}
}

// TestCrossFlowTieBrokenByFlowThenSeq performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestCrossFlowTieBrokenByFlowThenSeq(t *testing.T) {
	fa := model.Flow{SrcIP: 1, SrcPort: 1}
	fb := model.Flow{SrcIP: 1, SrcPort: 2} // same ip, higher port -> after fa
	orders := []*model.Order{
		{OrderID: "B", Flow: fb, TCPSeq: 5, T3Ns: 1000},
		{OrderID: "A", Flow: fa, TCPSeq: 5, T3Ns: 1000},
	}
	out := Order(orders)
	eq(t, ids(out), []string{"A", "B"})
}

// TestCrossFlowTieTolerance performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestCrossFlowTieTolerance(t *testing.T) {
	fa := model.Flow{SrcIP: 1, SrcPort: 1}
	fb := model.Flow{SrcIP: 2, SrcPort: 2}
	a := &model.Order{OrderID: "a", Flow: fa, EffectiveT3: 1000}
	bClose := &model.Order{OrderID: "b", Flow: fb, EffectiveT3: 1050} // Δ50 < 100
	bFar := &model.Order{OrderID: "b", Flow: fb, EffectiveT3: 1150}   // Δ150 >= 100
	sameFlow := &model.Order{OrderID: "c", Flow: fa, EffectiveT3: 1050}

	if !CrossFlowTie(a, bClose) {
		t.Error("Δ50 across flows must be a tie")
	}
	if CrossFlowTie(a, bFar) {
		t.Error("Δ150 across flows must NOT be a tie")
	}
	if CrossFlowTie(a, sameFlow) {
		t.Error("same-flow is never tolerated (strict tcp_seq order)")
	}
}
