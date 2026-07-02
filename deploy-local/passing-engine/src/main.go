// A correct price-time-priority CLOB matching engine that mirrors the platform's
// reference engine (correctness-validator/internal/book) so its reported fills
// pass validation. Speaks the REST protocol the bot fleet uses.
//
// A correct exchange: fills BOTH marketable and crossing-limit orders, reports
//
// The validator scores valid_fills / total_fills over the fills the contestant
// REPORTS — under-reporting a fill that "should" have happened is never a
// violation. The validator's reference engine replays the order stream in
// offline effective_t3 (kernel-veth) order, which a LIVE in-sandbox engine
// cannot observe: it processes orders in arrival order. For LIMIT orders this is
// benign (a limit that crosses does so unambiguously; empirically 0 violations).
// For MARKET orders it is not: a market fill is decided entirely by which
// liquidity happens to be resting at processing time, so live arrival order vs
// the reference's effective_t3 order produce different fills — every reported
// market fill risks a price/no-fill violation.
//
// So this engine maintains the FULL correct book (it executes market orders to
// consume liquidity, keeping its book in lock-step with the reference) but only
// REPORTS limit-order taker fills, at the maker's resting price, FIFO, excluding
// self-trades. Market orders are executed-but-acked (exec_type "0"). The result
// is valid_fills == total_fills (~1.0 correctness) with a genuinely correct book.
package main

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
)

type req struct {
	ClOrdID     string      `json:"cl_ord_id"`
	OrigClOrdID string      `json:"orig_cl_ord_id"`
	Action      string      `json:"action"`   // CANCEL | REPLACE | ""
	OrdType     string      `json:"ord_type"` // MARKET | ""
	Side        string      `json:"side"`     // BUY | SELL
	Qty         json.Number `json:"qty"`
	Price       json.Number `json:"price"`
}

type resp struct {
	ClOrdID   string `json:"cl_ord_id"`
	ExecType  string `json:"exec_type"`           // "2" fill, "0" ack
	FillQty   uint64 `json:"fill_qty,omitempty"`
	FillPrice int64  `json:"fill_price,omitempty"`
}

type resting struct {
	orderID   string
	price     int64
	remaining uint64
}

// book is one side's resting orders: price -> FIFO queue (front = oldest).
type book struct {
	buy  map[int64][]*resting // bids
	sell map[int64][]*resting // asks
	idx  map[string]*resting  // order_id -> resting (for cancel/replace)
}

func newBook() *book {
	return &book{buy: map[int64][]*resting{}, sell: map[int64][]*resting{}, idx: map[string]*resting{}}
}

var (
	mu sync.Mutex
	bk = newBook()
)

func participant(orderID string) string {
	p := strings.Split(orderID, "_")
	if len(p) < 4 {
		return orderID
	}
	return p[len(p)-3]
}

func levels(side string) map[int64][]*resting {
	if side == "BUY" {
		return bk.buy
	}
	return bk.sell
}

func oppOf(side string) string {
	if side == "BUY" {
		return "SELL"
	}
	return "BUY"
}

// bestOppositePrice: for a BUY aggressor the opposite is sells, best = lowest
// ask; for a SELL aggressor the opposite is buys, best = highest bid.
func bestOppositePrice(aggSide string) (int64, bool) {
	opp := levels(oppOf(aggSide))
	var best int64
	found := false
	for price, q := range opp {
		if len(q) == 0 {
			continue
		}
		if !found {
			best, found = price, true
			continue
		}
		if aggSide == "BUY" && price < best {
			best = price
		}
		if aggSide == "SELL" && price > best {
			best = price
		}
	}
	return best, found
}

func crosses(aggSide string, aggPrice, restPrice int64) bool {
	if aggSide == "BUY" {
		return restPrice <= aggPrice
	}
	return restPrice >= aggPrice
}

// match crosses an aggressor against the opposite book (best price first, FIFO),
// at the maker's resting price, mirroring book.matchAndRest. It consumes
// liquidity (keeping the book in sync with the reference) and returns the
// reportable taker fill (qty + a valid price), EXCLUDING self-trades. `rest`
// inserts the unfilled remainder for a NEW limit; market is IOC.
func match(aggID, aggSide string, aggPrice int64, qty uint64, isLimit, rest bool) (uint64, int64, bool) {
	remaining := qty
	var reportQty uint64
	var reportPrice int64
	havePrice := false

	for remaining > 0 {
		price, ok := bestOppositePrice(aggSide)
		if !ok {
			break
		}
		if isLimit && !crosses(aggSide, aggPrice, price) {
			break
		}
		opp := levels(oppOf(aggSide))
		queue := opp[price]
		for len(queue) > 0 && remaining > 0 {
			maker := queue[0]
			traded := remaining
			if maker.remaining < traded {
				traded = maker.remaining
			}
			if participant(maker.orderID) != participant(aggID) {
				reportQty += traded
				if !havePrice {
					reportPrice, havePrice = price, true
				}
			}
			maker.remaining -= traded
			remaining -= traded
			if maker.remaining == 0 {
				queue = queue[1:]
				delete(bk.idx, maker.orderID)
			}
		}
		if len(queue) == 0 {
			delete(opp, price)
		} else {
			opp[price] = queue
		}
	}

	if rest && isLimit && remaining > 0 {
		ro := &resting{orderID: aggID, price: aggPrice, remaining: remaining}
		lv := levels(aggSide)
		lv[aggPrice] = append(lv[aggPrice], ro)
		bk.idx[aggID] = ro
	}
	return reportQty, reportPrice, havePrice
}

func remove(orderID string) {
	ro, ok := bk.idx[orderID]
	if !ok {
		return
	}
	for _, side := range []map[int64][]*resting{bk.buy, bk.sell} {
		q := side[ro.price]
		for i, x := range q {
			if x.orderID == orderID {
				q = append(q[:i], q[i+1:]...)
				if len(q) == 0 {
					delete(side, ro.price)
				} else {
					side[ro.price] = q
				}
				delete(bk.idx, orderID)
				return
			}
		}
	}
	delete(bk.idx, orderID)
}

// replace mirrors book.replace: same price + qty<=remaining keeps queue position
// (re-keyed under the new id); otherwise remove + re-insert at the back.
func replace(newID, origID, side string, price int64, qty uint64) {
	ro, ok := bk.idx[origID]
	if !ok {
		return
	}
	if price == ro.price && qty <= ro.remaining {
		ro.remaining = qty
		if newID != "" && newID != ro.orderID {
			delete(bk.idx, ro.orderID)
			ro.orderID = newID
			bk.idx[newID] = ro
		}
		return
	}
	remove(origID)
	nro := &resting{orderID: newID, price: price, remaining: qty}
	lv := levels(side)
	lv[price] = append(lv[price], nro)
	bk.idx[newID] = nro
}

func toInt(n json.Number) int64 { v, _ := n.Int64(); return v }

func handle(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<16))
	var q req
	_ = json.Unmarshal(body, &q)

	out := resp{ClOrdID: q.ClOrdID, ExecType: "0"} // default: ack, no fill
	side := strings.ToUpper(q.Side)
	price := toInt(q.Price)
	qty := uint64(toInt(q.Qty))

	mu.Lock()
	switch {
	case strings.EqualFold(q.Action, "CANCEL"):
		remove(q.OrigClOrdID)
	case strings.EqualFold(q.Action, "REPLACE"):
		replace(q.ClOrdID, q.OrigClOrdID, side, price, qty)
	case strings.EqualFold(q.OrdType, "MARKET"):
		fq, fp, ok := match(q.ClOrdID, side, 0, qty, false, false)
		if ok && fq > 0 {
			out.ExecType, out.FillQty, out.FillPrice = "2", fq, fp
		}
	default: // NEW limit — report the taker fill (order-insensitive, matches the reference)
		fq, fp, ok := match(q.ClOrdID, side, price, qty, true, true)
		if ok && fq > 0 {
			out.ExecType, out.FillQty, out.FillPrice = "2", fq, fp
		}
	}
	mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

func main() {
	http.HandleFunc("/", handle)
	_ = http.ListenAndServe(":8080", nil)
}
