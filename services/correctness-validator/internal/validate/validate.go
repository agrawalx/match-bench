// Package validate implements validate behavior.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package validate

import (
	"fmt"

	"github.com/iicpc/correctness-validator/internal/book"
	"github.com/iicpc/correctness-validator/internal/model"
	"github.com/iicpc/correctness-validator/internal/replay"
)

type ViolationType string

const (
	Phantom           ViolationType = "phantom"
	Overfill          ViolationType = "overfill"
	Price             ViolationType = "price"
	Time              ViolationType = "time"
	SelfTrade         ViolationType = "self_trade"
	CancelReplaceLoss ViolationType = "cancel_replace_loss"
	// LostOrder/LostCancel are invariants-mode-only (VALIDATOR_MODE=invariants):
	// an order/cancel sent on the wire that never got any response.
	LostOrder  ViolationType = "lost_order"
	LostCancel ViolationType = "lost_cancel"
)

// Violation groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type Violation struct {
	Type          ViolationType
	OrderID       string
	ReportedQty   uint64
	ReportedPrice int64
	Detail        string
}

// ReportedFill groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type ReportedFill struct {
	OrderID string
	Qty     uint64
	Price   int64
}

// Report groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type Report struct {
	TotalFills      uint64
	ValidFills      uint64
	PhantomFills    uint64
	Overfills       uint64
	PriceViolations uint64
	TimeViolations  uint64
	SelfTrades      uint64
	Violations      []Violation

	// totalViolations is the exact count of every add() call, independent of how
	// many example Violation structs got retained in Violations (see add,
	// maxViolationExamplesPerType). ViolationCount() reports this, not
	// len(Violations), so capping the example slice for memory never makes the
	// reported/stored violation_count metric lie.
	totalViolations uint64
	// violationExamplesByType caps how many example Violation structs are retained
	// per ViolationType in Violations (N=100 each, maxViolationExamplesPerType). The
	// exact counters (Overfills, PriceViolations, TimeViolations, SelfTrades,
	// PhantomFills, LostOrders, LostCancels, and totalViolations above) are never
	// capped and stay authoritative; this map only bounds the O(violations) memory
	// the example slice would otherwise grow to on a long adversarial run.
	violationExamplesByType map[ViolationType]int

	// Invariants-mode-only fields (zero in full-replay mode).
	LostOrders  uint64
	LostCancels uint64
	Jitter      JitterStats
	// T7ReorderLate counts orders whose minT7Ns arrived after the T7 reorder
	// window had already evicted (and released) that point in the timeline. These
	// are excluded from the incremental FIFO/cross-flow checks (their ordering
	// slot is gone) but must not crash or silently vanish.
	T7ReorderLate uint64

	// ScoredFills is the denominator for CorrectnessScore. Full mode's Run counts
	// phantom fills into TotalFills (its phantom loop increments both TotalFills and
	// PhantomFills together), so ScoredFills there is TotalFills-PhantomFills.
	// Invariants mode's TotalFills counts real fill responses only — AddUnmatched
	// increments PhantomFills separately as an unmatched_responses metric and never
	// touches TotalFills — so ScoredFills there is just TotalFills. Both modes set
	// this field directly so CorrectnessScore never has to guess which subtraction
	// applies (and never underflows the uint64 subtraction that used to live here).
	ScoredFills uint64
}

// CorrectnessScore applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (r Report) CorrectnessScore() float64 {
	// Phantom-fill is removed as a scored violation class (docs/multi-contestant-audit.md
	// §5, decision 2026-07-16): a fabricated fill is already excluded from the assembled
	// order set upstream (it fails the orders.sent join), so PhantomFills is kept as the
	// unmatched_responses metric only and is not counted against the score. ScoredFills
	// is set by the caller (Run for full mode, InvariantsValidator.Finish for
	// invariants mode) because the two modes disagree on whether TotalFills already
	// includes phantom fills (see the field doc comment).
	if r.ScoredFills == 0 {
		return 1.0
	}
	return float64(r.ValidFills) / float64(r.ScoredFills)
}

// ViolationCount applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (r Report) ViolationCount() uint64 {
	return r.totalViolations
}

// maxViolationExamplesPerType caps how many Violation example structs are retained
// per ViolationType in Report.Violations, so the example slice stays O(1) per type
// (O(#types) overall) instead of growing unbounded with session length. The exact
// per-type counters (and totalViolations/ViolationCount) are unaffected by this cap.
const maxViolationExamplesPerType = 100

// isFill performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func isFill(execType string, qty uint64) bool {
	return qty > 0 && (execType == "1" || execType == "2" || execType == "F")
}

// refFills groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type refFills struct {
	qty    uint64
	prices map[int64]struct{}
}

// Run performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func Run(ordered []*model.Order, phantoms []ReportedFill) Report {
	engine := book.NewEngine()
	for _, o := range ordered {
		engine.Process(o)
	}

	ref := make(map[string]*refFills)
	for _, f := range engine.Fills() {
		r := ref[f.OrderID]
		if r == nil {
			r = &refFills{prices: make(map[int64]struct{})}
			ref[f.OrderID] = r
		}
		r.qty += f.Qty
		r.prices[f.Price] = struct{}{}
	}

	tradesByOrder := make(map[string][]book.Trade)
	for _, tr := range engine.Trades() {
		tradesByOrder[tr.MakerOrderID] = append(tradesByOrder[tr.MakerOrderID], tr)
		tradesByOrder[tr.TakerOrderID] = append(tradesByOrder[tr.TakerOrderID], tr)
	}

	orderByID := make(map[string]*model.Order, len(ordered))
	for _, o := range ordered {
		orderByID[o.OrderID] = o
	}

	availByLevel := make(map[levelKey][]book.Availability)
	if AggressiveFillToleranceNs > 0 {
		for _, a := range engine.Availability() {
			k := levelKey{a.Side, a.Price}
			availByLevel[k] = append(availByLevel[k], a)
		}
	}
	tolerated := func(o *model.Order, price int64) bool {
		if AggressiveFillToleranceNs == 0 {
			return false
		}
		me := model.ParticipantOf(o.OrderID)
		for _, a := range availByLevel[levelKey{oppositeOf(o.Side), price}] {
			if a.Participant == me {
				continue
			}
			if windowsOverlap(a.EnterT3, a.ExitT3, o.EffectiveT3, AggressiveFillToleranceNs) {
				return true
			}
		}
		return false
	}

	var rep Report

	for _, o := range ordered {
		var cumReported uint64
		for _, resp := range o.Responses {
			if !isFill(resp.ExecType, resp.FillQty) {
				continue
			}
			rep.TotalFills++
			cumReported += resp.FillQty
			price := int64(resp.FillPrice)

			switch {
			case cumReported > o.Qty:
				rep.Overfills++
				rep.add(Overfill, o.OrderID, resp.FillQty, price,
					fmt.Sprintf("cumulative reported %d exceeds order qty %d", cumReported, o.Qty))

			case selfTrade(tradesByOrder, o.OrderID, price):
				rep.SelfTrades++
				rep.add(SelfTrade, o.OrderID, resp.FillQty, price,
					"reference match for this fill has the same participant (bot_id) on both sides")

			case ref[o.OrderID] == nil:
				if jumper, ok := queueJump(engine, orderByID, o, price); ok {
					rep.flagJump(engine, o, jumper, resp.FillQty, price)
				} else if tolerated(o, price) {
					rep.ValidFills++
				} else {
					rep.PriceViolations++
					rep.add(Price, o.OrderID, resp.FillQty, price,
						"reference engine produced no fill for this order")
				}

			case !priceAllowed(ref[o.OrderID], price):
				if tolerated(o, price) {
					rep.ValidFills++
				} else {
					rep.PriceViolations++
					rep.add(Price, o.OrderID, resp.FillQty, price,
						"reported fill price not produced by the reference engine for this order")
				}

			case cumReported > ref[o.OrderID].qty:
				if jumper, ok := queueJump(engine, orderByID, o, price); ok {
					rep.flagJump(engine, o, jumper, resp.FillQty, price)
				} else if tolerated(o, price) {
					rep.ValidFills++
				} else {
					rep.PriceViolations++
					rep.add(Price, o.OrderID, resp.FillQty, price,
						fmt.Sprintf("cumulative reported %d exceeds reference fill qty %d", cumReported, ref[o.OrderID].qty))
				}

			default:
				rep.ValidFills++
			}
		}
	}

	for _, pf := range phantoms {
		rep.TotalFills++
		rep.PhantomFills++
		rep.add(Phantom, pf.OrderID, pf.Qty, pf.Price,
			"fill reported for an order_id that was never sent")
	}

	rep.ScoredFills = rep.TotalFills - rep.PhantomFills

	return rep
}

// add applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (r *Report) add(t ViolationType, id string, qty uint64, price int64, detail string) {
	r.totalViolations++
	if r.violationExamplesByType == nil {
		r.violationExamplesByType = make(map[ViolationType]int)
	}
	if r.violationExamplesByType[t] >= maxViolationExamplesPerType {
		return
	}
	r.violationExamplesByType[t]++
	r.Violations = append(r.Violations, Violation{
		Type: t, OrderID: id, ReportedQty: qty, ReportedPrice: price, Detail: detail,
	})
}

// flagJump applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (r *Report) flagJump(e *book.Engine, o *model.Order, jumper string, qty uint64, price int64) {
	r.TimeViolations++
	if e.Repriced(o.OrderID) {
		r.add(CancelReplaceLoss, o.OrderID, qty, price,
			fmt.Sprintf("repriced order filled ahead of %s, which was already resting at the new level (lost time priority on REPLACE)", jumper))
		return
	}
	r.add(Time, o.OrderID, qty, price,
		fmt.Sprintf("filled ahead of earlier same-price order %s (time priority)", jumper))
}

// selfTrade performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func selfTrade(tradesByOrder map[string][]book.Trade, orderID string, price int64) bool {
	me := model.ParticipantOf(orderID)
	for _, tr := range tradesByOrder[orderID] {
		if tr.Price != price {
			continue
		}
		counterparty := tr.MakerOrderID
		if tr.MakerOrderID == orderID {
			counterparty = tr.TakerOrderID
		}
		if model.ParticipantOf(counterparty) == me {
			return true
		}
	}
	return false
}

// queueJump performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func queueJump(e *book.Engine, orderByID map[string]*model.Order, o *model.Order, price int64) (string, bool) {
	mySeq, ok := e.SeqOf(o.OrderID)
	if !ok {
		return "", false
	}
	var (
		best    string
		bestSeq uint64
		found   bool
	)
	for id, other := range orderByID {
		if id == o.OrderID || other.Side != o.Side || other.Price != price {
			continue
		}
		otherSeq, ok := e.SeqOf(id)
		if !ok || otherSeq >= mySeq {
			continue
		}
		if replay.CrossFlowTie(o, other) {
			continue
		}
		if !found || otherSeq < bestSeq || (otherSeq == bestSeq && id < best) {
			best, bestSeq, found = id, otherSeq, true
		}
	}
	return best, found
}

// priceAllowed performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func priceAllowed(r *refFills, price int64) bool {
	_, ok := r.prices[price]
	return ok
}

var AggressiveFillToleranceNs uint64

// levelKey groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type levelKey struct {
	side  model.Side
	price int64
}

// oppositeOf performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func oppositeOf(s model.Side) model.Side {
	if s == model.Buy {
		return model.Sell
	}
	return model.Buy
}

// windowsOverlap performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func windowsOverlap(enter, exit, t3, tol uint64) bool {
	lo := uint64(0)
	if t3 > tol {
		lo = t3 - tol
	}
	hi := t3 + tol
	return enter <= hi && exit >= lo
}
