// Package book defines tests for trade test.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package book

import (
	"testing"

	"github.com/iicpc/correctness-validator/internal/model"
)

// TestTradesCarryMakerTaker performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestTradesCarryMakerTaker(t *testing.T) {
	e := NewEngine()
	e.Process(limit("S1", model.Sell, 100, 10))
	e.Process(limit("B1", model.Buy, 100, 10))
	tr := e.Trades()
	if len(tr) != 1 {
		t.Fatalf("expected 1 trade, got %d: %+v", len(tr), tr)
	}
	got := tr[0]
	if got.MakerOrderID != "S1" || got.TakerOrderID != "B1" {
		t.Fatalf("maker/taker wrong: %+v", got)
	}
	if got.Price != 100 || got.Qty != 10 {
		t.Fatalf("price/qty wrong: %+v", got)
	}
}

// TestTradesPreserveFIFOMakerOrder performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestTradesPreserveFIFOMakerOrder(t *testing.T) {
	e := NewEngine()
	e.Process(limit("S1", model.Sell, 100, 5))
	e.Process(limit("S2", model.Sell, 100, 5))
	e.Process(market("B", model.Buy, 8))
	tr := e.Trades()
	if len(tr) != 2 {
		t.Fatalf("expected 2 trades, got %d: %+v", len(tr), tr)
	}
	if tr[0].MakerOrderID != "S1" || tr[0].Qty != 5 {
		t.Fatalf("first trade should fully consume S1: %+v", tr[0])
	}
	if tr[1].MakerOrderID != "S2" || tr[1].Qty != 3 {
		t.Fatalf("second trade should partially consume S2: %+v", tr[1])
	}
}

// TestRestingSnapshot performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestRestingSnapshot(t *testing.T) {
	e := NewEngine()
	e.Process(limit("S1", model.Sell, 100, 10))
	e.Process(limit("S2", model.Sell, 100, 10))
	rest := e.Resting()
	r1, ok1 := rest["S1"]
	r2, ok2 := rest["S2"]
	if !ok1 || !ok2 {
		t.Fatalf("both S1 and S2 should be resting: %+v", rest)
	}
	if r1.Price != 100 || r1.Remaining != 10 || r2.Remaining != 10 {
		t.Fatalf("resting state wrong: %+v %+v", r1, r2)
	}
	if !(r1.Seq < r2.Seq) {
		t.Fatalf("S1 must have an earlier FIFO seq than S2: %d vs %d", r1.Seq, r2.Seq)
	}
}
