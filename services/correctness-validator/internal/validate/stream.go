// Streaming, bounded-memory validation. Where validate.Run (batch) buffers the whole
// session, replays the book, then compares in a second pass, StreamValidator does it
// single-pass: it feeds orders into the engine in EffectiveT3 order and finalizes each
// order's correctness check the moment it leaves the reference book (fully filled,
// cancelled, replaced) — or at session end for orders that rest forever. Per-order
// reference fills are accumulated from the engine's drained fills/trades and discarded
// on finalize, so memory is O(live book), not O(session).
//
// It reproduces validate.Run's per-order semantics exactly for the default
// (AggressiveFillToleranceNs == 0) path. The one intentional refinement: queue-jump
// (time-priority) is checked against the orders actually RESTING ahead of the filled
// order — the real queue — rather than every order that ever existed at that price.
package validate

import (
	"fmt"

	"github.com/iicpc/correctness-validator/internal/book"
	"github.com/iicpc/correctness-validator/internal/model"
	"github.com/iicpc/correctness-validator/internal/replay"
)

// orderRef is one order's accumulated reference fills, built incrementally from the
// engine's drained fills/trades while the order is live, then consumed on finalize.
type orderRef struct {
	qty        uint64
	prices     map[int64]struct{}
	selfPrices map[int64]struct{}
}

// StreamValidator validates a session incrementally with bounded memory.
type StreamValidator struct {
	engine  *book.Engine
	pending map[string]*model.Order // orders in the book (or being processed), keyed by id
	ref     map[string]*orderRef    // per-live-order accumulated reference fills
	rep     Report
}

// NewStreamValidator constructs an empty streaming validator.
func NewStreamValidator() *StreamValidator {
	return &StreamValidator{
		engine:  book.NewEngine(),
		pending: make(map[string]*model.Order),
		ref:     make(map[string]*orderRef),
	}
}

// Apply feeds the next order (in EffectiveT3 order) into the reference engine and
// finalizes any orders that left the book as a result.
func (v *StreamValidator) Apply(o *model.Order) {
	v.pending[o.OrderID] = o
	v.engine.Process(o)

	for _, f := range v.engine.DrainFills() {
		r := v.ref[f.OrderID]
		if r == nil {
			r = &orderRef{prices: make(map[int64]struct{}), selfPrices: make(map[int64]struct{})}
			v.ref[f.OrderID] = r
		}
		r.qty += f.Qty
		r.prices[f.Price] = struct{}{}
	}
	for _, t := range v.engine.DrainTrades() {
		if model.ParticipantOf(t.MakerOrderID) == model.ParticipantOf(t.TakerOrderID) {
			v.markSelf(t.MakerOrderID, t.Price)
			v.markSelf(t.TakerOrderID, t.Price)
		}
	}

	for _, id := range v.engine.DrainEvicted() {
		v.finalize(id)
	}
	// An order that neither rested nor was evicted (a taker that fully filled, a
	// market order, a cancel/replace action whose own id never rests) is finalized now.
	if _, stillPending := v.pending[o.OrderID]; stillPending && !v.engine.IsResting(o.OrderID) {
		v.finalize(o.OrderID)
	}
}

// Finish finalizes every order still resting (it never left the book), folds in the
// phantom fills, and returns the report.
func (v *StreamValidator) Finish() Report {
	ids := make([]string, 0, len(v.pending))
	for id := range v.pending {
		ids = append(ids, id)
	}
	for _, id := range ids {
		v.finalize(id)
	}
	return v.rep
}

// AddPhantom records a fill reported for an order_id that was never sent.
func (v *StreamValidator) AddPhantom(pf ReportedFill) {
	v.rep.TotalFills++
	v.rep.PhantomFills++
	v.rep.add(Phantom, pf.OrderID, pf.Qty, pf.Price, "fill reported for an order_id that was never sent")
}

func (v *StreamValidator) markSelf(orderID string, price int64) {
	r := v.ref[orderID]
	if r == nil {
		r = &orderRef{prices: make(map[int64]struct{}), selfPrices: make(map[int64]struct{})}
		v.ref[orderID] = r
	}
	r.selfPrices[price] = struct{}{}
}

func (v *StreamValidator) finalize(id string) {
	o := v.pending[id]
	if o == nil {
		return // already finalized (or never tracked)
	}
	delete(v.pending, id)
	r := v.ref[id]
	delete(v.ref, id)
	v.scoreOrder(o, r)
	v.engine.Forget(id)
}

// scoreOrder is the per-order comparison — the body of validate.Run's second pass,
// driven by this order's accumulated reference fills (r) and the live book.
func (v *StreamValidator) scoreOrder(o *model.Order, r *orderRef) {
	var cumReported uint64
	for _, resp := range o.Responses {
		if !isFill(resp.ExecType, resp.FillQty) {
			continue
		}
		v.rep.TotalFills++
		cumReported += resp.FillQty
		price := int64(resp.FillPrice)

		switch {
		case cumReported > o.Qty:
			v.rep.Overfills++
			v.rep.add(Overfill, o.OrderID, resp.FillQty, price,
				fmt.Sprintf("cumulative reported %d exceeds order qty %d", cumReported, o.Qty))

		case r != nil && hasPrice(r.selfPrices, price):
			v.rep.SelfTrades++
			v.rep.add(SelfTrade, o.OrderID, resp.FillQty, price,
				"reference match for this fill has the same participant (bot_id) on both sides")

		case r == nil:
			if jumper, ok := v.queueJump(o, price); ok {
				v.rep.flagJump(v.engine, o, jumper, resp.FillQty, price)
			} else {
				v.rep.PriceViolations++
				v.rep.add(Price, o.OrderID, resp.FillQty, price,
					"reference engine produced no fill for this order")
			}

		case !hasPrice(r.prices, price):
			v.rep.PriceViolations++
			v.rep.add(Price, o.OrderID, resp.FillQty, price,
				"reported fill price not produced by the reference engine for this order")

		case cumReported > r.qty:
			if jumper, ok := v.queueJump(o, price); ok {
				v.rep.flagJump(v.engine, o, jumper, resp.FillQty, price)
			} else {
				v.rep.PriceViolations++
				v.rep.add(Price, o.OrderID, resp.FillQty, price,
					fmt.Sprintf("cumulative reported %d exceeds reference fill qty %d", cumReported, r.qty))
			}

		default:
			v.rep.ValidFills++
		}
	}
}

// queueJump finds the earliest order still RESTING ahead of o at the same price+side
// (lower seq, not a cross-flow tie) — the order that should have had time priority.
func (v *StreamValidator) queueJump(o *model.Order, price int64) (string, bool) {
	mySeq, ok := v.engine.SeqOf(o.OrderID)
	if !ok {
		return "", false // o never rested, so it never queued
	}
	var (
		best    string
		bestSeq uint64
		found   bool
	)
	for id, other := range v.pending {
		if id == o.OrderID || other.Side != o.Side || other.Price != price {
			continue
		}
		otherSeq, ok := v.engine.SeqOf(id)
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

func hasPrice(m map[int64]struct{}, p int64) bool {
	_, ok := m[p]
	return ok
}
