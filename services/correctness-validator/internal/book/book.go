// Package book implements book behavior.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package book

import (
	"math"

	"github.com/google/btree"
	"github.com/iicpc/correctness-validator/internal/model"
)

// Fill groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type Fill struct {
	OrderID string
	Price   int64
	Qty     uint64
}

// Trade groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type Trade struct {
	MakerOrderID string
	TakerOrderID string
	Price        int64
	Qty          uint64
	MakerSeq     uint64
}

// RestingState groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type RestingState struct {
	OrderID   string
	Side      model.Side
	Price     int64
	Seq       uint64
	Remaining uint64
}

// restingOrder groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type restingOrder struct {
	orderID   string
	side      model.Side
	price     int64
	remaining uint64
	seq       uint64 // arrival rank (FIFO/time priority within a level)
	availIdx  int    // index into Engine.avail for this resting order's window
}

// Availability groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type Availability struct {
	Side        model.Side
	Price       int64
	Participant string
	EnterT3     uint64
	ExitT3      uint64
}

// priceLevel groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type priceLevel struct {
	price  int64
	orders []*restingOrder // FIFO: front = oldest = time priority
}

// Engine groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type Engine struct {
	asks       *btree.BTreeG[*priceLevel] // ascending: best ask = Min
	bids       *btree.BTreeG[*priceLevel] // descending: best bid = Min (highest price)
	index      map[string]*restingOrder
	seq        uint64
	fills      []Fill
	trades     []Trade
	repriced   map[string]bool
	seqByOrder map[string]uint64
	avail      []Availability
	// evicted accumulates the order IDs removed from the book during the current
	// Process call (maker fully consumed, cancel, replace-remove). The streaming
	// validator drains it after each Process to finalize departed orders. The batch
	// path never drains it (small: ids only) and is being retired.
	evicted []string
}

// NewEngine performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func NewEngine() *Engine {
	return &Engine{
		asks:       btree.NewG[*priceLevel](32, func(a, b *priceLevel) bool { return a.price < b.price }),
		bids:       btree.NewG[*priceLevel](32, func(a, b *priceLevel) bool { return a.price > b.price }),
		index:      make(map[string]*restingOrder),
		repriced:   make(map[string]bool),
		seqByOrder: make(map[string]uint64),
	}
}

// Repriced applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (e *Engine) Repriced(orderID string) bool { return e.repriced[orderID] }

// SeqOf applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (e *Engine) SeqOf(orderID string) (uint64, bool) {
	s, ok := e.seqByOrder[orderID]
	return s, ok
}

// Fills applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (e *Engine) Fills() []Fill { return e.fills }

// Trades applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (e *Engine) Trades() []Trade { return e.trades }

// DrainFills returns the fills produced since the last drain and clears the buffer,
// so a streaming consumer attributes them per-order without the engine retaining
// O(session) fills. (Batch callers use Fills() and never drain.)
func (e *Engine) DrainFills() []Fill {
	f := e.fills
	e.fills = nil
	return f
}

// DrainTrades returns + clears trades produced since the last drain (see DrainFills).
func (e *Engine) DrainTrades() []Trade {
	t := e.trades
	e.trades = nil
	return t
}

// DrainEvicted returns + clears the order IDs removed from the book during the
// Process call(s) since the last drain, so the streaming validator can finalize them.
func (e *Engine) DrainEvicted() []string {
	ev := e.evicted
	e.evicted = nil
	return ev
}

// Forget drops the per-order metadata (seq, repriced) for an order the streaming
// validator has finalized, bounding those maps to live orders. Batch never calls it.
func (e *Engine) Forget(orderID string) {
	delete(e.seqByOrder, orderID)
	delete(e.repriced, orderID)
}

// IsResting reports whether the order is currently in the book.
func (e *Engine) IsResting(orderID string) bool {
	_, ok := e.index[orderID]
	return ok
}

// Resting applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (e *Engine) Resting() map[string]RestingState {
	out := make(map[string]RestingState, len(e.index))
	for id, ro := range e.index {
		out[id] = RestingState{
			OrderID: id, Side: ro.side, Price: ro.price, Seq: ro.seq, Remaining: ro.remaining,
		}
	}
	return out
}

// tree applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (e *Engine) tree(side model.Side) *btree.BTreeG[*priceLevel] {
	if side == model.Buy {
		return e.bids
	}
	return e.asks
}

// Process applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (e *Engine) Process(o *model.Order) {
	switch o.Kind {
	case model.NewLimit:
		e.matchAndRest(o, true)
	case model.NewMarket:
		e.matchAndRest(o, false)
	case model.Cancel:
		e.remove(o.OrigOrderID, o.EffectiveT3)
	case model.Replace:
		e.replace(o)
	}
}

// matchAndRest applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (e *Engine) matchAndRest(o *model.Order, rest bool) {
	remaining := o.Qty
	opp := oppositeSide(o.Side)
	oppTree := e.tree(opp)

	for remaining > 0 {
		level, ok := oppTree.Min()
		if !ok {
			break
		}
		if o.Kind == model.NewLimit && !crosses(o.Side, o.Price, level.price) {
			break
		}
		for len(level.orders) > 0 && remaining > 0 {
			maker := level.orders[0]
			traded := min(remaining, maker.remaining)
			e.fills = append(e.fills,
				Fill{OrderID: o.OrderID, Price: level.price, Qty: traded},
				Fill{OrderID: maker.orderID, Price: level.price, Qty: traded},
			)
			e.trades = append(e.trades, Trade{
				MakerOrderID: maker.orderID,
				TakerOrderID: o.OrderID,
				Price:        level.price,
				Qty:          traded,
				MakerSeq:     maker.seq,
			})
			maker.remaining -= traded
			remaining -= traded
			if maker.remaining == 0 {
				level.orders = level.orders[1:]
				delete(e.index, maker.orderID)
				e.evicted = append(e.evicted, maker.orderID)
				e.closeAvail(maker, o.EffectiveT3) // maker fully consumed at the aggressor's t3
			}
		}
		if len(level.orders) == 0 {
			oppTree.Delete(level)
		}
	}

	if rest && remaining > 0 && o.Kind == model.NewLimit {
		e.insert(o, remaining)
	}
}

// insert applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (e *Engine) insert(o *model.Order, remaining uint64) {
	e.seq++
	ro := &restingOrder{orderID: o.OrderID, side: o.Side, price: o.Price, remaining: remaining, seq: e.seq}
	ro.availIdx = len(e.avail)
	e.avail = append(e.avail, Availability{
		Side: o.Side, Price: o.Price, Participant: model.ParticipantOf(o.OrderID),
		EnterT3: o.EffectiveT3, ExitT3: math.MaxUint64,
	})
	e.index[o.OrderID] = ro
	e.seqByOrder[o.OrderID] = e.seq
	tree := e.tree(o.Side)
	key := &priceLevel{price: o.Price}
	if level, ok := tree.Get(key); ok {
		level.orders = append(level.orders, ro)
	} else {
		key.orders = []*restingOrder{ro}
		tree.ReplaceOrInsert(key)
	}
}

// closeAvail applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (e *Engine) closeAvail(ro *restingOrder, exitT3 uint64) {
	if ro.availIdx >= 0 && ro.availIdx < len(e.avail) && e.avail[ro.availIdx].ExitT3 == math.MaxUint64 {
		e.avail[ro.availIdx].ExitT3 = exitT3
	}
}

// Availability applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (e *Engine) Availability() []Availability { return e.avail }

// remove applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (e *Engine) remove(orderID string, exitT3 uint64) {
	ro, ok := e.index[orderID]
	if !ok {
		return
	}
	e.closeAvail(ro, exitT3)
	tree := e.tree(ro.side)
	if level, ok := tree.Get(&priceLevel{price: ro.price}); ok {
		for i, x := range level.orders {
			if x.orderID == orderID {
				level.orders = append(level.orders[:i], level.orders[i+1:]...)
				break
			}
		}
		if len(level.orders) == 0 {
			tree.Delete(level)
		}
	}
	delete(e.index, orderID)
	e.evicted = append(e.evicted, orderID)
}

// replace applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (e *Engine) replace(o *model.Order) {
	ro, ok := e.index[o.OrigOrderID]
	if !ok {
		return
	}
	if o.Price == ro.price && o.Qty <= ro.remaining {
		ro.remaining = o.Qty
		if o.OrderID != "" && o.OrderID != ro.orderID {
			oldID := ro.orderID
			delete(e.index, oldID)
			ro.orderID = o.OrderID
			e.index[o.OrderID] = ro
			if s, ok := e.seqByOrder[oldID]; ok {
				e.seqByOrder[o.OrderID] = s
				delete(e.seqByOrder, oldID)
			}
		}
		return
	}
	if o.Price != ro.price {
		e.repriced[orReplaceID(o)] = true
	}
	e.remove(o.OrigOrderID, o.EffectiveT3)
	e.insert(&model.Order{OrderID: orReplaceID(o), Side: o.Side, Price: o.Price}, o.Qty)
}

// orReplaceID performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func orReplaceID(o *model.Order) string {
	if o.OrderID != "" {
		return o.OrderID
	}
	return o.OrigOrderID
}

// crosses performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func crosses(aggressorSide model.Side, aggressorPrice, restingPrice int64) bool {
	if aggressorSide == model.Buy {
		return restingPrice <= aggressorPrice
	}
	return restingPrice >= aggressorPrice
}

// oppositeSide performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func oppositeSide(s model.Side) model.Side {
	if s == model.Buy {
		return model.Sell
	}
	return model.Buy
}

// min performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func min(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}
