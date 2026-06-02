package validate

import (
	"reflect"
	"testing"

	"github.com/iicpc/correctness-validator/internal/model"
	"github.com/iicpc/correctness-validator/internal/replay"
)

// ordWithFlow is like the test helper `order` but lets a scenario set the flow,
// tcp_seq and t3 so ordering-sensitive checks (time priority / cross-flow tie) can
// be exercised. effective_t3 is computed by replay.Order, mirroring production.
func ordWithFlow(id string, kind model.Kind, side model.Side, price int64, qty uint64, flow model.Flow, seq uint32, t3 uint64, resp ...model.Response) *model.Order {
	return &model.Order{
		OrderID: id, Kind: kind, Side: side, Price: price, Qty: qty,
		Flow: flow, TCPSeq: seq, T3Ns: t3, Responses: resp,
	}
}

func replaceOrd(id, orig string, side model.Side, price int64, qty uint64, resp ...model.Response) *model.Order {
	return &model.Order{OrderID: id, OrigOrderID: orig, Kind: model.Replace, Side: side, Price: price, Qty: qty, Responses: resp}
}

// ---- TIME PRIORITY -----------------------------------------------------------

// Two same-price resting sells: S1 (FIFO front) then S2. A single buy of 5 should,
// per price-time priority, fill S1. If the contestant instead reports S2 filling
// while S1 still rests with full qty, S2 jumped the queue -> time-priority violation.
func TestTimePriorityViolationFlagged(t *testing.T) {
	fa := model.Flow{SrcIP: 1, SrcPort: 1}
	ordered := []*model.Order{
		ordWithFlow("S1", model.NewLimit, model.Sell, 100, 10, fa, 1, 10),                // front, no fill reported
		ordWithFlow("S2", model.NewLimit, model.Sell, 100, 10, fa, 2, 20, fill(5, 100)),  // behind S1, but reports a fill
		ordWithFlow("B1", model.NewLimit, model.Buy, 100, 5, fa, 3, 30, fill(5, 100)),    // taker buys 5
	}
	ordered = replay.Order(ordered)
	r := Run(ordered, nil)
	if r.TimeViolations != 1 {
		t.Fatalf("expected 1 time-priority violation, got %+v", r)
	}
	if r.Violations[0].Type != Time {
		t.Fatalf("expected Time violation type, got %v (%+v)", r.Violations[0].Type, r.Violations)
	}
}

// Clean FIFO: the front order S1 fills, S2 untouched -> no time violation.
func TestTimePriorityCleanNotFlagged(t *testing.T) {
	fa := model.Flow{SrcIP: 1, SrcPort: 1}
	ordered := []*model.Order{
		ordWithFlow("S1", model.NewLimit, model.Sell, 100, 10, fa, 1, 10, fill(5, 100)),
		ordWithFlow("S2", model.NewLimit, model.Sell, 100, 10, fa, 2, 20),
		ordWithFlow("B1", model.NewLimit, model.Buy, 100, 5, fa, 3, 30, fill(5, 100)),
	}
	ordered = replay.Order(ordered)
	r := Run(ordered, nil)
	if r.TimeViolations != 0 {
		t.Fatalf("clean FIFO must not flag a time violation, got %+v", r)
	}
}

// ---- SELF-TRADE --------------------------------------------------------------

// Maker and taker order_ids embed the SAME bot_id -> the reported fill is a
// self-trade. order_id = {session}_{bot}_{seq}_{suffix}.
func TestSelfTradeViolationFlagged(t *testing.T) {
	ordered := []*model.Order{
		order("sess_7_1_O", model.NewLimit, model.Sell, 100, 10, fill(10, 100)), // bot 7 maker
		order("sess_7_2_O", model.NewLimit, model.Buy, 100, 10, fill(10, 100)),  // bot 7 taker — SELF TRADE
	}
	r := Run(ordered, nil)
	if r.SelfTrades == 0 {
		t.Fatalf("expected a self-trade violation, got %+v", r)
	}
	foundSelf := false
	for _, v := range r.Violations {
		if v.Type == SelfTrade {
			foundSelf = true
		}
	}
	if !foundSelf {
		t.Fatalf("expected a SelfTrade violation entry, got %+v", r.Violations)
	}
}

// Different bot_ids on the two sides -> a normal trade, NOT a self-trade.
func TestSelfTradeCleanNotFlagged(t *testing.T) {
	ordered := []*model.Order{
		order("sess_7_1_O", model.NewLimit, model.Sell, 100, 10, fill(10, 100)), // bot 7 maker
		order("sess_9_2_O", model.NewLimit, model.Buy, 100, 10, fill(10, 100)),  // bot 9 taker
	}
	r := Run(ordered, nil)
	if r.SelfTrades != 0 {
		t.Fatalf("distinct participants must not be a self-trade, got %+v", r)
	}
}

// ---- CANCEL-REPLACE PRIORITY LOSS -------------------------------------------

// B_old rests @100. B_other rests @99 (already at the level). REPLACE moves B_old
// to 99 -> it must go to the BACK, behind B_other. A sell @99 of 10 fills B_other
// first per FIFO. If the contestant reports B_old_R filling (jumping ahead of the
// earlier-resting B_other), it kept its old priority -> cancel-replace loss.
func TestCancelReplacePriorityLossFlagged(t *testing.T) {
	fa := model.Flow{SrcIP: 1, SrcPort: 1}
	ordered := []*model.Order{
		ordWithFlow("B_old", model.NewLimit, model.Buy, 100, 10, fa, 1, 10),
		ordWithFlow("B_other", model.NewLimit, model.Buy, 99, 10, fa, 2, 20),
		// REPLACE B_old: price 100 -> 99 (loses priority, goes behind B_other).
		// Contestant wrongly reports B_old_R filling at 99.
		{OrderID: "B_old_R", OrigOrderID: "B_old", Kind: model.Replace, Side: model.Buy, Price: 99, Qty: 10,
			Flow: fa, TCPSeq: 3, T3Ns: 30, Responses: []model.Response{fill(10, 99)}},
		ordWithFlow("S1", model.NewLimit, model.Sell, 99, 10, fa, 4, 40, fill(10, 99)),
	}
	ordered = replay.Order(ordered)
	r := Run(ordered, nil)
	if r.TimeViolations == 0 {
		t.Fatalf("expected a cancel-replace priority-loss violation, got %+v", r)
	}
	found := false
	for _, v := range r.Violations {
		if v.Type == CancelReplaceLoss {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected a CancelReplaceLoss violation entry, got %+v", r.Violations)
	}
}

// A qty-only decrease keeps priority. S1 stays at the front; reporting its fill is
// legitimate -> no cancel-replace violation.
func TestCancelReplaceQtyDecreaseKeepsPriority(t *testing.T) {
	fa := model.Flow{SrcIP: 1, SrcPort: 1}
	ordered := []*model.Order{
		ordWithFlow("S1", model.NewLimit, model.Sell, 100, 10, fa, 1, 10),
		ordWithFlow("S2", model.NewLimit, model.Sell, 100, 10, fa, 2, 20),
		// REPLACE S1: qty 10 -> 5, SAME price -> keeps front position.
		replaceOrd("S1_R", "S1", model.Sell, 100, 5, fill(5, 100)),
		ordWithFlow("B1", model.NewLimit, model.Buy, 100, 5, fa, 4, 40, fill(5, 100)),
	}
	// give S1_R a flow/seq so replay ordering is well-defined
	ordered[2].Flow, ordered[2].TCPSeq, ordered[2].T3Ns = fa, 3, 30
	ordered = replay.Order(ordered)
	r := Run(ordered, nil)
	if r.TimeViolations != 0 || violationsOfType(r, CancelReplaceLoss) != 0 {
		t.Fatalf("qty-only decrease keeps priority; must not flag, got %+v", r)
	}
}

// ---- 100ns CROSS-FLOW TIE TOLERANCE -----------------------------------------

// Same scenario as the time-priority violation, but S1 and S2 are on DIFFERENT
// flows. Δeffective_t3 = 50ns < 100ns tolerance -> the contestant was free to order
// them either way, so the queue-jump must be SUPPRESSED.
func TestCrossFlowTieSuppressesTimeViolation(t *testing.T) {
	fa := model.Flow{SrcIP: 1, SrcPort: 1}
	fb := model.Flow{SrcIP: 2, SrcPort: 2}
	ordered := []*model.Order{
		ordWithFlow("S1", model.NewLimit, model.Sell, 100, 10, fa, 1, 1000),               // effective_t3 1000
		ordWithFlow("S2", model.NewLimit, model.Sell, 100, 10, fb, 1, 1050, fill(5, 100)), // effective_t3 1050, Δ50
		ordWithFlow("B1", model.NewLimit, model.Buy, 100, 5, fa, 2, 2000, fill(5, 100)),
	}
	ordered = replay.Order(ordered)
	r := Run(ordered, nil)
	if r.TimeViolations != 0 {
		t.Fatalf("Δ50ns cross-flow tie must suppress the time violation, got %+v", r)
	}
}

// Δeffective_t3 = 150ns >= 100ns -> NOT a tie; the queue-jump IS flagged.
func TestCrossFlowBeyondToleranceFlagsTimeViolation(t *testing.T) {
	fa := model.Flow{SrcIP: 1, SrcPort: 1}
	fb := model.Flow{SrcIP: 2, SrcPort: 2}
	ordered := []*model.Order{
		ordWithFlow("S1", model.NewLimit, model.Sell, 100, 10, fa, 1, 1000),               // effective_t3 1000
		ordWithFlow("S2", model.NewLimit, model.Sell, 100, 10, fb, 1, 1150, fill(5, 100)), // effective_t3 1150, Δ150
		ordWithFlow("B1", model.NewLimit, model.Buy, 100, 5, fa, 2, 2000, fill(5, 100)),
	}
	ordered = replay.Order(ordered)
	r := Run(ordered, nil)
	if r.TimeViolations != 1 {
		t.Fatalf("Δ150ns is beyond tolerance; the time violation must be flagged, got %+v", r)
	}
}

// Same-flow ordering is strict (no tolerance): a later same-price order jumping the
// queue is always flagged regardless of how small the t3 gap is.
func TestSameFlowStrictNoTolerance(t *testing.T) {
	fa := model.Flow{SrcIP: 1, SrcPort: 1}
	ordered := []*model.Order{
		ordWithFlow("S1", model.NewLimit, model.Sell, 100, 10, fa, 1, 1000),
		ordWithFlow("S2", model.NewLimit, model.Sell, 100, 10, fa, 2, 1001, fill(5, 100)), // Δ1ns, same flow
		ordWithFlow("B1", model.NewLimit, model.Buy, 100, 5, fa, 3, 2000, fill(5, 100)),
	}
	ordered = replay.Order(ordered)
	r := Run(ordered, nil)
	if r.TimeViolations != 1 {
		t.Fatalf("same-flow ordering is strict; a 1ns gap must still flag, got %+v", r)
	}
}

// ---- DETERMINISM (full synthetic session, run twice, byte-identical) ---------

func TestReportDeterministicFullSession(t *testing.T) {
	build := func() []*model.Order {
		fa := model.Flow{SrcIP: 1, SrcPort: 1}
		fb := model.Flow{SrcIP: 2, SrcPort: 2}
		o := []*model.Order{
			ordWithFlow("sess_1_1_O", model.NewLimit, model.Sell, 100, 10, fa, 1, 10, fill(10, 100)),
			ordWithFlow("sess_2_2_O", model.NewLimit, model.Buy, 100, 10, fb, 1, 20, fill(10, 100)),
			ordWithFlow("sess_1_3_O", model.NewLimit, model.Sell, 101, 5, fa, 2, 30),
			ordWithFlow("sess_3_4_O", model.NewLimit, model.Buy, 100, 5, fa, 3, 40, fill(5, 100)), // jumps queue maybe
			ordWithFlow("sess_3_5_O", model.NewLimit, model.Buy, 200, 100, fb, 2, 50, fill(100, 200)),
		}
		return replay.Order(o)
	}
	r1 := Run(build(), []ReportedFill{{OrderID: "ghost", Qty: 1, Price: 50}})
	r2 := Run(build(), []ReportedFill{{OrderID: "ghost", Qty: 1, Price: 50}})
	if !reflect.DeepEqual(r1, r2) {
		t.Fatalf("CorrectnessReport not deterministic:\n r1=%+v\n r2=%+v", r1, r2)
	}
}

func violationsOfType(r Report, vt ViolationType) int {
	n := 0
	for _, v := range r.Violations {
		if v.Type == vt {
			n++
		}
	}
	return n
}
