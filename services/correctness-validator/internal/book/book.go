// Package book is the reference matching engine: a canonical price-time-priority
// CLOB. It replays the delivery-ordered request stream and produces the fills a
// correct exchange WOULD have generated, recorded per order_id for BOTH sides of
// every trade. The validator diffs the contestant's reported per-order fills
// against these reference fills — which sidesteps the maker/taker attribution
// problem, because the reference engine knows every order (from orders.sent) and
// assigns each trade's qty/price to both the taker and the maker order_id.
package book

import (
	"github.com/google/btree"
	"github.com/iicpc/correctness-validator/internal/model"
)

// Fill is one reference execution attributed to a single order_id (a trade
// produces two: one for the taker, one for the maker), at the maker's resting
// price (price-time priority).
type Fill struct {
	OrderID string
	Price   int64
	Qty     uint64
}

// Trade is one reference match with both sides identified. The maker is the
// resting order the FIFO/price-time engine picked (NOT taken from the contestant's
// report — the validator infers the maker from the reference book), and the price
// is always the maker's resting price. MakerSeq is the maker's FIFO arrival rank,
// which lets the validator reason about queue position for time-priority and
// cancel-replace checks.
type Trade struct {
	MakerOrderID string
	TakerOrderID string
	Price        int64
	Qty          uint64
	MakerSeq     uint64
}

// RestingState is the end-of-replay snapshot of one order still in the book: its
// side, price, FIFO arrival rank, and quantity the reference engine left unfilled.
// The validator uses it to ask "was there an earlier same-price order the
// reference left with remaining qty?" (time priority) and "what was already
// resting at the new price level before this replace?" (cancel-replace).
type RestingState struct {
	OrderID   string
	Side      model.Side
	Price     int64
	Seq       uint64
	Remaining uint64
}

type restingOrder struct {
	orderID   string
	side      model.Side
	price     int64
	remaining uint64
	seq       uint64 // arrival rank (FIFO/time priority within a level)
}

type priceLevel struct {
	price  int64
	orders []*restingOrder // FIFO: front = oldest = time priority
}

// Engine is the reference CLOB. Not safe for concurrent use (one per session).
type Engine struct {
	asks   *btree.BTreeG[*priceLevel] // ascending: best ask = Min
	bids   *btree.BTreeG[*priceLevel] // descending: best bid = Min (highest price)
	index  map[string]*restingOrder
	seq    uint64
	fills  []Fill
	trades []Trade
	// repriced records order_ids that lost time priority via a price-changing
	// REPLACE (moved to the back of the new price level). A reported fill that
	// jumps an earlier same-price order is classified as a cancel-replace priority
	// loss (rather than a plain time-priority break) when the jumping order is here.
	repriced map[string]bool
	// seqByOrder is the FIFO arrival rank assigned to every order that ever rested,
	// retained after the order leaves the book. Two same-(side,price) orders with
	// seqByOrder[a] < seqByOrder[b] mean a arrived (and must fill) before b — the
	// ground truth for time-priority and cancel-replace queue-position checks.
	seqByOrder map[string]uint64
}

func NewEngine() *Engine {
	return &Engine{
		asks:     btree.NewG[*priceLevel](32, func(a, b *priceLevel) bool { return a.price < b.price }),
		bids:     btree.NewG[*priceLevel](32, func(a, b *priceLevel) bool { return a.price > b.price }),
		index:      make(map[string]*restingOrder),
		repriced:   make(map[string]bool),
		seqByOrder: make(map[string]uint64),
	}
}

// Repriced reports whether order_id lost time priority via a price-changing
// REPLACE (it was moved to the back of the new price level).
func (e *Engine) Repriced(orderID string) bool { return e.repriced[orderID] }

// SeqOf returns the FIFO arrival rank of an order that ever rested (retained even
// after it left the book), and whether the order ever rested at all.
func (e *Engine) SeqOf(orderID string) (uint64, bool) {
	s, ok := e.seqByOrder[orderID]
	return s, ok
}

// Fills returns every reference fill produced so far (taker + maker entries).
func (e *Engine) Fills() []Fill { return e.fills }

// Trades returns every reference match with both sides identified, in match order.
func (e *Engine) Trades() []Trade { return e.trades }

// Resting returns the end-of-replay book: order_id -> its unfilled resting state.
func (e *Engine) Resting() map[string]RestingState {
	out := make(map[string]RestingState, len(e.index))
	for id, ro := range e.index {
		out[id] = RestingState{
			OrderID: id, Side: ro.side, Price: ro.price, Seq: ro.seq, Remaining: ro.remaining,
		}
	}
	return out
}

func (e *Engine) tree(side model.Side) *btree.BTreeG[*priceLevel] {
	if side == model.Buy {
		return e.bids
	}
	return e.asks
}

// Process applies one delivery-ordered request to the book, generating reference
// fills. NEW limit crosses then rests; NEW market is IOC (no rest); CANCEL/REPLACE
// act on the referenced order.
func (e *Engine) Process(o *model.Order) {
	switch o.Kind {
	case model.NewLimit:
		e.matchAndRest(o, true)
	case model.NewMarket:
		e.matchAndRest(o, false)
	case model.Cancel:
		e.remove(o.OrigOrderID)
	case model.Replace:
		e.replace(o)
	}
}

// matchAndRest crosses an aggressor against the opposite book FIFO/price-first,
// recording a fill for both sides of each trade; `rest` controls whether an
// unfilled remainder is inserted (limit) or dropped (market, IOC).
func (e *Engine) matchAndRest(o *model.Order, rest bool) {
	remaining := o.Qty
	opp := oppositeSide(o.Side)
	oppTree := e.tree(opp)

	for remaining > 0 {
		level, ok := oppTree.Min()
		if !ok {
			break
		}
		// Price gate for limit orders: a buy can only take asks <= its price; a
		// sell can only take bids >= its price. Market orders ignore the gate.
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

func (e *Engine) insert(o *model.Order, remaining uint64) {
	e.seq++
	ro := &restingOrder{orderID: o.OrderID, side: o.Side, price: o.Price, remaining: remaining, seq: e.seq}
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

func (e *Engine) remove(orderID string) {
	ro, ok := e.index[orderID]
	if !ok {
		return
	}
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
}

// replace modifies the referenced order. Price change -> back of the new level
// (time priority lost). Same price, qty decrease -> in place (priority kept).
// Same price, qty increase -> back of level (conservative CLOB rule).
func (e *Engine) replace(o *model.Order) {
	ro, ok := e.index[o.OrigOrderID]
	if !ok {
		return // can't replace an order that isn't resting (already filled/cancelled)
	}
	if o.Price == ro.price && o.Qty <= ro.remaining {
		ro.remaining = o.Qty // qty-only decrease: keep position
		// re-key the index under the new order id (the replace carries a new ClOrdID)
		if o.OrderID != "" && o.OrderID != ro.orderID {
			oldID := ro.orderID
			delete(e.index, oldID)
			ro.orderID = o.OrderID
			e.index[o.OrderID] = ro
			// A qty-decrease replace KEEPS queue position, so the new ClOrdID must
			// inherit the original FIFO arrival rank — otherwise SeqOf(newID) misses
			// and queueJump can't classify a later violation involving this order as
			// time-priority / cancel-replace-loss (it would fall through to the wrong
			// violation type; the fill stays flagged, only the label is wrong).
			if s, ok := e.seqByOrder[oldID]; ok {
				e.seqByOrder[o.OrderID] = s
				delete(e.seqByOrder, oldID)
			}
		}
		return
	}
	// price change or qty increase: remove and re-insert at the back of the level.
	// A price change is the case that loses priority by amendment intent; record it
	// so the validator can classify a queue-jump on it as a cancel-replace loss.
	if o.Price != ro.price {
		e.repriced[orReplaceID(o)] = true
	}
	e.remove(o.OrigOrderID)
	e.insert(&model.Order{OrderID: orReplaceID(o), Side: o.Side, Price: o.Price}, o.Qty)
}

func orReplaceID(o *model.Order) string {
	if o.OrderID != "" {
		return o.OrderID
	}
	return o.OrigOrderID
}

func crosses(aggressorSide model.Side, aggressorPrice, restingPrice int64) bool {
	if aggressorSide == model.Buy {
		return restingPrice <= aggressorPrice
	}
	return restingPrice >= aggressorPrice
}

func oppositeSide(s model.Side) model.Side {
	if s == model.Buy {
		return model.Sell
	}
	return model.Buy
}

func min(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}
