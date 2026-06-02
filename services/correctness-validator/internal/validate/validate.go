// Package validate diffs the contestant's REPORTED fills (from orders.acked)
// against the reference matching engine's fills, classifying each reported fill
// and scoring valid/total.
//
// The reference engine knows every order, so it produces the correct fill for
// every order_id. We compare per order_id (qty + the set of prices the order
// traded at), which is decidable without maker↔taker pairing:
//   - phantom        — a reported fill on an order_id that was never sent.
//   - overfill       — reported cumulative qty exceeds the order's qty.
//   - price          — a reported fill at a price the reference never gave this
//     order (captures price-priority breaks and fills that
//     shouldn't have happened), or qty beyond what the reference
//     produced for it.
//
// Time-priority and self-trade are NOT separately decidable without the maker
// ClOrdID in the execution report; with the per-order-price model they surface as
// `price` violations when the traded price differs, and are otherwise out of
// scope for v1 (the 0.99 DQ slop absorbs validator imperfection).
package validate

import (
	"fmt"

	"github.com/iicpc/correctness-validator/internal/book"
	"github.com/iicpc/correctness-validator/internal/model"
)

type ViolationType string

const (
	Phantom  ViolationType = "phantom"
	Overfill ViolationType = "overfill"
	Price    ViolationType = "price"
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
	// Reference fills per order_id.
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
				rep.Violations = append(rep.Violations, Violation{
					Type: Overfill, OrderID: o.OrderID, ReportedQty: resp.FillQty, ReportedPrice: price,
					Detail: fmt.Sprintf("cumulative reported %d exceeds order qty %d", cumReported, o.Qty),
				})
			case ref[o.OrderID] == nil:
				// Order was sent but the reference engine never filled it.
				rep.PriceViolations++
				rep.Violations = append(rep.Violations, Violation{
					Type: Price, OrderID: o.OrderID, ReportedQty: resp.FillQty, ReportedPrice: price,
					Detail: "reference engine produced no fill for this order",
				})
			case !priceAllowed(ref[o.OrderID], price):
				rep.PriceViolations++
				rep.Violations = append(rep.Violations, Violation{
					Type: Price, OrderID: o.OrderID, ReportedQty: resp.FillQty, ReportedPrice: price,
					Detail: "reported fill price not produced by the reference engine for this order",
				})
			case cumReported > ref[o.OrderID].qty:
				rep.PriceViolations++
				rep.Violations = append(rep.Violations, Violation{
					Type: Price, OrderID: o.OrderID, ReportedQty: resp.FillQty, ReportedPrice: price,
					Detail: fmt.Sprintf("cumulative reported %d exceeds reference fill qty %d", cumReported, ref[o.OrderID].qty),
				})
			default:
				rep.ValidFills++
			}
		}
	}

	for _, pf := range phantoms {
		rep.TotalFills++
		rep.PhantomFills++
		rep.Violations = append(rep.Violations, Violation{
			Type: Phantom, OrderID: pf.OrderID, ReportedQty: pf.Qty, ReportedPrice: pf.Price,
			Detail: "fill reported for an order_id that was never sent",
		})
	}

	return rep
}

func priceAllowed(r *refFills, price int64) bool {
	_, ok := r.prices[price]
	return ok
}
