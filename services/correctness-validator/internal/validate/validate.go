// Package validate diffs the contestant's REPORTED fills (from orders.acked)
// against a reference matching engine, classifying each reported fill and scoring
// valid/total.
//
// The reference engine (internal/book) replays the delivery-ordered request stream
// and, for every aggressor, emits the trades it WOULD produce — each with a
// definite maker/taker pair (the maker is INFERRED from the reference book's FIFO,
// not taken from the contestant's report, so no maker ClOrdID is needed on the
// wire), the maker's resting price, and the FIFO arrival rank of every order that
// ever rested. We reconcile the reported per-order fills against these reference
// facts and classify each divergence:
//
//   - phantom            — a reported fill on an order_id that was never sent.
//   - overfill           — reported cumulative qty exceeds the order's own qty.
//   - self-trade         — the reference trade for this reported fill has the same
//     participant (bot_id, parsed from order_id) on both sides.
//   - time priority      — the order reports a fill it should not have, because an
//     earlier-arriving same-(side,price) order had queue priority (FIFO front).
//   - cancel-replace loss— a time-priority break where the jumping order lost its
//     priority via a price-changing REPLACE (must go to the back of the new level).
//   - price              — a reported fill at a price the reference never gave this
//     order, or qty beyond what the reference produced for it.
//
// Ordering-sensitive checks (time priority / cancel-replace) honour the 100ns
// cross-flow tie tolerance from internal/replay: when the two orders involved are
// on different flows and their effective_t3 differ by less than the tolerance, the
// contestant was free to order them either way and the break is suppressed. Within
// a single flow the byte stream is unambiguous, so the check is strict.
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
)

type Violation struct {
	Type          ViolationType
	OrderID       string
	ReportedQty   uint64
	ReportedPrice int64
	Detail        string
}

// ReportedFill is an acked fill whose order_id was never sent (phantom input,
// detected at the join stage).
type ReportedFill struct {
	OrderID string
	Qty     uint64
	Price   int64
}

type Report struct {
	TotalFills      uint64
	ValidFills      uint64
	PhantomFills    uint64
	Overfills       uint64
	PriceViolations uint64
	TimeViolations  uint64
	SelfTrades      uint64
	Violations      []Violation
}

func (r Report) CorrectnessScore() float64 {
	if r.TotalFills == 0 {
		return 1.0 // nothing to get wrong
	}
	return float64(r.ValidFills) / float64(r.TotalFills)
}

func (r Report) ViolationCount() uint32 {
	return uint32(len(r.Violations))
}

func isFill(execType string, qty uint64) bool {
	return qty > 0 && (execType == "1" || execType == "2" || execType == "F")
}

type refFills struct {
	qty    uint64
	prices map[int64]struct{}
}

// Run replays `ordered` (delivery-ordered sent orders, each carrying its acked
// Responses) through the reference engine and validates the reported fills.
// `phantoms` are acked fills with no matching sent order (detected upstream).
func Run(ordered []*model.Order, phantoms []ReportedFill) Report {
	engine := book.NewEngine()
	for _, o := range ordered {
		engine.Process(o)
	}

	// Per-order reference fills (qty + the price set), used for the price/qty checks.
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

	// Reference trades indexed by each side's order_id, so a reported fill on an
	// order can find the counterparty (self-trade) at a given price.
	tradesByOrder := make(map[string][]book.Trade)
	for _, tr := range engine.Trades() {
		tradesByOrder[tr.MakerOrderID] = append(tradesByOrder[tr.MakerOrderID], tr)
		tradesByOrder[tr.TakerOrderID] = append(tradesByOrder[tr.TakerOrderID], tr)
	}

	orderByID := make(map[string]*model.Order, len(ordered))
	for _, o := range ordered {
		orderByID[o.OrderID] = o
	}

	// Aggressive-fill tolerance index. A LIVE in-sandbox engine processes orders
	// in socket-arrival order, not the offline effective_t3 order the reference
	// replays, so a market/crossing-limit order legitimately matches liquidity at
	// a slightly different instant. When AggressiveFillToleranceNs > 0, a reported
	// aggressive fill at price P is accepted if NON-SELF opposite liquidity at P
	// was genuinely resting within ±tolerance of the aggressor's effective_t3 —
	// the fairness analog of the resting-order cross-flow tie tolerance. Default 0
	// preserves strict behavior. Overfill and self-trade are never tolerated.
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
				// Reference never filled this order. If an earlier same-(side,price)
				// order had queue priority, the contestant jumped the queue; otherwise
				// the fill simply shouldn't have happened (price/liquidity break).
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
				// Reported beyond what the reference gave this order at a valid price.
				// If an earlier same-(side,price) order had priority for that extra
				// quantity, it is a queue jump; otherwise an over-reported price break.
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

	return rep
}

func (r *Report) add(t ViolationType, id string, qty uint64, price int64, detail string) {
	r.Violations = append(r.Violations, Violation{
		Type: t, OrderID: id, ReportedQty: qty, ReportedPrice: price, Detail: detail,
	})
}

// flagJump records a queue-jump as a cancel-replace priority loss when the jumping
// order lost priority via a price-changing REPLACE, otherwise a plain time break.
// Both increment TimeViolations (they are the two faces of the time-priority gate).
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

// queueJump reports whether order `o`, reporting a fill at `price`, jumped ahead of
// an earlier same-(side,price) order that had FIFO priority. It returns the earlier
// order's id. The 100ns cross-flow tie tolerance suppresses the break when `o` and
// the earlier order are on different flows within the tolerance window.
func queueJump(e *book.Engine, orderByID map[string]*model.Order, o *model.Order, price int64) (string, bool) {
	mySeq, ok := e.SeqOf(o.OrderID)
	if !ok {
		return "", false // o never rested in the reference book, so it never queued
	}
	// Deterministically pick the FIFO-front competitor: smallest arrival seq, then
	// smallest id. Map iteration order is unstable, so we cannot return first-match.
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
			continue // not earlier in the queue
		}
		if replay.CrossFlowTie(o, other) {
			continue // cross-flow tie within tolerance: contestant free to order either way
		}
		if !found || otherSeq < bestSeq || (otherSeq == bestSeq && id < best) {
			best, bestSeq, found = id, otherSeq, true
		}
	}
	return best, found
}

func priceAllowed(r *refFills, price int64) bool {
	_, ok := r.prices[price]
	return ok
}

// AggressiveFillToleranceNs is the ±window (effective_t3 nanoseconds) within which
// a market/crossing-limit fill is accepted if it matched genuine non-self opposite
// liquidity at the reported price. 0 = strict (no tolerance), preserving the
// original behavior. The validator's main sets it from AGGRESSIVE_FILL_TOLERANCE_NS.
// It exists because a live in-sandbox engine processes in socket-arrival order, not
// the reference's offline effective_t3 order, so aggressive fills are otherwise
// penalized for an ordering the contestant could not observe. Resting-order time
// priority keeps its own (tighter) cross-flow tie tolerance in internal/replay.
var AggressiveFillToleranceNs uint64

// levelKey indexes the reference liquidity timeline by (side, price).
type levelKey struct {
	side  model.Side
	price int64
}

func oppositeOf(s model.Side) model.Side {
	if s == model.Buy {
		return model.Sell
	}
	return model.Buy
}

// windowsOverlap reports whether a resting order's availability window
// [enter, exit] overlaps the aggressor's tolerance window [t3-tol, t3+tol],
// guarding against unsigned underflow on t3-tol.
func windowsOverlap(enter, exit, t3, tol uint64) bool {
	lo := uint64(0)
	if t3 > tol {
		lo = t3 - tol
	}
	hi := t3 + tol
	return enter <= hi && exit >= lo
}
