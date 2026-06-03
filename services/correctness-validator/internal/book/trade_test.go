package book

import (
	"testing"

	"github.com/iicpc/correctness-validator/internal/model"
)

// The engine must emit, per trade, a definite maker/taker pair at the maker's
// resting price — this is what the validator reconciles reported fills against to
// decide time-priority, self-trade, and cancel-replace violations.
func TestTradesCarryMakerTaker(t *testing.T) {
	e := NewEngine()
	e.Process(limit("S1", model.Sell, 100, 10)) // maker rests
	e.Process(limit("B1", model.Buy, 100, 10))  // taker crosses
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

// FIFO at a level: the front (earliest) maker trades before the next, and each
// trade records WHICH maker the reference filled. This is the ground truth a
// time-priority check compares the contestant's report against.
func TestTradesPreserveFIFOMakerOrder(t *testing.T) {
	e := NewEngine()
	e.Process(limit("S1", model.Sell, 100, 5)) // front
	e.Process(limit("S2", model.Sell, 100, 5)) // behind S1
	e.Process(market("B", model.Buy, 8))       // takes all of S1, then 3 of S2
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

// Resting() exposes the end-of-replay book so the validator can ask "was there an
// earlier same-price order the reference left with remaining qty?" (time priority)
// and "what was already resting at the new level before a replace?" (cancel-replace).
func TestRestingSnapshot(t *testing.T) {
	e := NewEngine()
	e.Process(limit("S1", model.Sell, 100, 10)) // stays resting (front, seq smaller)
	e.Process(limit("S2", model.Sell, 100, 10)) // stays resting (behind S1)
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
