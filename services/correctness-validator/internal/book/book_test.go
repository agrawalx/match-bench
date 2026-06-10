// Package book defines tests for book test.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package book

import (
	"testing"

	"github.com/iicpc/correctness-validator/internal/model"
)

// limit performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func limit(id string, side model.Side, price int64, qty uint64) *model.Order {
	return &model.Order{OrderID: id, Side: side, Price: price, Qty: qty, Kind: model.NewLimit}
}

// market performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func market(id string, side model.Side, qty uint64) *model.Order {
	return &model.Order{OrderID: id, Side: side, Qty: qty, Kind: model.NewMarket}
}

// cancel performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func cancel(orig string) *model.Order {
	return &model.Order{OrderID: orig + "_C", OrigOrderID: orig, Kind: model.Cancel}
}

// replace performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func replace(id, orig string, side model.Side, price int64, qty uint64) *model.Order {
	return &model.Order{OrderID: id, OrigOrderID: orig, Side: side, Price: price, Qty: qty, Kind: model.Replace}
}

// filled performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func filled(e *Engine) map[string]uint64 {
	m := map[string]uint64{}
	for _, f := range e.Fills() {
		m[f.OrderID] += f.Qty
	}
	return m
}

// priceOf performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func priceOf(e *Engine, id string) int64 {
	price := int64(-1)
	for _, f := range e.Fills() {
		if f.OrderID == id {
			if price != -1 && price != f.Price {
				return -2 // multiple prices
			}
			price = f.Price
		}
	}
	return price
}

// TestLimitCross performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestLimitCross(t *testing.T) {
	e := NewEngine()
	e.Process(limit("S1", model.Sell, 100, 10)) // rests
	e.Process(limit("B1", model.Buy, 100, 10))  // crosses, fully fills
	f := filled(e)
	if f["B1"] != 10 || f["S1"] != 10 {
		t.Fatalf("expected both filled 10, got %v", f)
	}
	if priceOf(e, "B1") != 100 || priceOf(e, "S1") != 100 {
		t.Fatalf("expected fills at maker price 100")
	}
}

// TestMarketWalksLevelsFIFO performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestMarketWalksLevelsFIFO(t *testing.T) {
	e := NewEngine()
	e.Process(limit("S1", model.Sell, 100, 5)) // best level, first
	e.Process(limit("S2", model.Sell, 100, 5)) // same level, second (FIFO after S1)
	e.Process(limit("S3", model.Sell, 101, 10))
	e.Process(market("B", model.Buy, 12))
	f := filled(e)
	if f["S1"] != 5 || f["S2"] != 5 || f["S3"] != 2 || f["B"] != 12 {
		t.Fatalf("market walk wrong: %v", f)
	}
	var sawS2BeforeS1Done bool
	s1 := uint64(0)
	for _, fl := range e.Fills() {
		if fl.OrderID == "S1" {
			s1 += fl.Qty
		}
		if fl.OrderID == "S2" && s1 < 5 {
			sawS2BeforeS1Done = true
		}
	}
	if sawS2BeforeS1Done {
		t.Fatal("time priority violated: S2 filled before S1 exhausted")
	}
}

// TestPricePriorityLowestAskFirst performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestPricePriorityLowestAskFirst(t *testing.T) {
	e := NewEngine()
	e.Process(limit("Shi", model.Sell, 101, 10))
	e.Process(limit("Slo", model.Sell, 100, 10))
	e.Process(market("B", model.Buy, 5))
	f := filled(e)
	if f["Slo"] != 5 || f["Shi"] != 0 {
		t.Fatalf("price priority violated: expected Slo filled, got %v", f)
	}
}

// TestNonMarketableLimitRestsThenFills performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestNonMarketableLimitRestsThenFills(t *testing.T) {
	e := NewEngine()
	e.Process(limit("Sask", model.Sell, 100, 10))
	e.Process(limit("Bbid", model.Buy, 99, 10)) // below ask -> rests, no fill
	if len(e.Fills()) != 0 {
		t.Fatalf("non-marketable limit should not fill, got %v", e.Fills())
	}
	e.Process(limit("Scross", model.Sell, 99, 10)) // hits the resting bid
	f := filled(e)
	if f["Bbid"] != 10 || f["Scross"] != 10 {
		t.Fatalf("resting bid should fill when crossed: %v", f)
	}
}

// TestPartialFillKeepsMakerFront performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestPartialFillKeepsMakerFront(t *testing.T) {
	e := NewEngine()
	e.Process(limit("S1", model.Sell, 100, 10))
	e.Process(limit("B1", model.Buy, 100, 4)) // partial: S1 leaves 6 resting
	e.Process(limit("B2", model.Buy, 100, 6)) // fills S1's remaining 6
	f := filled(e)
	if f["S1"] != 10 || f["B1"] != 4 || f["B2"] != 6 {
		t.Fatalf("partial fill accounting wrong: %v", f)
	}
}

// TestCancelRemovesResting performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestCancelRemovesResting(t *testing.T) {
	e := NewEngine()
	e.Process(limit("S1", model.Sell, 100, 10))
	e.Process(cancel("S1"))
	e.Process(market("B", model.Buy, 10)) // book empty -> no fill
	if len(e.Fills()) != 0 {
		t.Fatalf("cancelled order must not fill, got %v", e.Fills())
	}
}

// TestReplacePriceChangeMovesLevel performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestReplacePriceChangeMovesLevel(t *testing.T) {
	e := NewEngine()
	e.Process(limit("B1", model.Buy, 100, 10))          // bid @100
	e.Process(replace("B1_R", "B1", model.Buy, 99, 10)) // reprice down to 99
	e.Process(limit("S@100", model.Sell, 100, 10))      // should NOT hit a 99 bid
	if len(e.Fills()) != 0 {
		t.Fatalf("sell@100 must not fill a bid repriced to 99, got %v", e.Fills())
	}
	e.Process(limit("S@99", model.Sell, 99, 10)) // now crosses the repriced bid
	f := filled(e)
	if f["B1_R"] != 10 || f["S@99"] != 10 {
		t.Fatalf("repriced bid should fill at 99: %v", f)
	}
}

// TestReplaceQtyDecreaseKeepsPriority performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestReplaceQtyDecreaseKeepsPriority(t *testing.T) {
	e := NewEngine()
	e.Process(limit("S1", model.Sell, 100, 10))
	e.Process(limit("S2", model.Sell, 100, 10))          // behind S1
	e.Process(replace("S1_R", "S1", model.Sell, 100, 5)) // S1 qty 10->5, keeps front
	e.Process(market("B", model.Buy, 6))                 // takes S1's 5 then S2's 1
	f := filled(e)
	if f["S1_R"] != 5 || f["S2"] != 1 {
		t.Fatalf("qty-decrease must keep front priority: %v", f)
	}
}

// TestReplaceQtyDecreaseCarriesFIFORank performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestReplaceQtyDecreaseCarriesFIFORank(t *testing.T) {
	e := NewEngine()
	e.Process(limit("A", model.Sell, 100, 10)) // rests first
	e.Process(limit("B", model.Sell, 100, 10)) // same level, behind A
	seqA, ok := e.SeqOf("A")
	if !ok {
		t.Fatal("A must have a FIFO rank after resting")
	}
	e.Process(replace("A_R", "A", model.Sell, 100, 8)) // qty 10->8, keeps front

	got, ok := e.SeqOf("A_R")
	if !ok {
		t.Fatal("after qty-decrease replace, SeqOf(new id) must resolve (regression: seqByOrder not re-keyed)")
	}
	if got != seqA {
		t.Errorf("re-keyed order rank = %d, want %d (must keep queue position)", got, seqA)
	}
	if _, stale := e.SeqOf("A"); stale {
		t.Error("old id must no longer carry a FIFO rank after re-key")
	}
	if seqB, _ := e.SeqOf("B"); !(got < seqB) {
		t.Errorf("re-keyed order rank %d must stay ahead of B's %d (time priority preserved)", got, seqB)
	}
}
