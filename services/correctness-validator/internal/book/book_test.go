package book

import (
	"testing"

	"github.com/iicpc/correctness-validator/internal/model"
)

func limit(id string, side model.Side, price int64, qty uint64) *model.Order {
	return &model.Order{OrderID: id, Side: side, Price: price, Qty: qty, Kind: model.NewLimit}
}
func market(id string, side model.Side, qty uint64) *model.Order {
	return &model.Order{OrderID: id, Side: side, Qty: qty, Kind: model.NewMarket}
}
func cancel(orig string) *model.Order {
	return &model.Order{OrderID: orig + "_C", OrigOrderID: orig, Kind: model.Cancel}
}
func replace(id, orig string, side model.Side, price int64, qty uint64) *model.Order {
	return &model.Order{OrderID: id, OrigOrderID: orig, Side: side, Price: price, Qty: qty, Kind: model.Replace}
}

// filled returns total reference-filled qty per order_id.
func filled(e *Engine) map[string]uint64 {
	m := map[string]uint64{}
	for _, f := range e.Fills() {
		m[f.OrderID] += f.Qty
	}
	return m
}

// priceOf returns the (single) price an order traded at, or -1 if it never traded
// or traded at multiple prices.
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
	// FIFO within the 100 level: S1 must be fully consumed before S2.
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

func TestCancelRemovesResting(t *testing.T) {
	e := NewEngine()
	e.Process(limit("S1", model.Sell, 100, 10))
	e.Process(cancel("S1"))
	e.Process(market("B", model.Buy, 10)) // book empty -> no fill
	if len(e.Fills()) != 0 {
		t.Fatalf("cancelled order must not fill, got %v", e.Fills())
	}
}

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
