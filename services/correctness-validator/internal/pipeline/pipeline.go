// Package pipeline implements pipeline behavior.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package pipeline

import (
	"github.com/iicpc/correctness-validator/internal/model"
	"github.com/iicpc/correctness-validator/internal/replay"
	"github.com/iicpc/correctness-validator/internal/validate"
	"github.com/iicpc/schemas/topics"
)

// isFill performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func isFill(execType string, qty uint64) bool {
	return qty > 0 && (execType == "1" || execType == "2" || execType == "F")
}

// AssembleOrder joins one sent event with its acks into a model.Order, identically
// to Assemble (so the batch and streaming paths build orders the same way). Returns
// nil if the order was never delivered (no acks). EffectiveT3 is left zero; the caller
// sets it (replay.Order in batch, the promotion pass in the streaming source).
func AssembleOrder(s topics.OrderSentEvent, acks []topics.OrderAckedEvent) *model.Order {
	if len(acks) == 0 {
		return nil
	}
	first := acks[0]
	return &model.Order{
		OrderID:     s.OrderID,
		Flow:        model.Flow{SrcIP: first.SrcIP, SrcPort: first.SrcPort},
		TCPSeq:      first.TCPSeq,
		T3Ns:        first.T3XDPIngressNS,
		Side:        model.SideFrom(s.Side),
		Price:       int64(s.Price) * int64(topics.TelemetryPriceScale),
		Qty:         s.Qty,
		Kind:        model.KindFrom(s.PayloadType, s.OrdType),
		OrigOrderID: preferOrig(s.OrigOrderID, acks),
		Responses:   responses(acks),
	}
}

// Assemble performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func Assemble(sents []topics.OrderSentEvent, ackeds []topics.OrderAckedEvent) ([]*model.Order, []validate.ReportedFill) {
	sentByID := make(map[string]topics.OrderSentEvent, len(sents))
	for _, s := range sents {
		sentByID[s.OrderID] = s
	}
	ackedByID := make(map[string][]topics.OrderAckedEvent)
	for _, a := range ackeds {
		ackedByID[a.OrderID] = append(ackedByID[a.OrderID], a)
	}

	var orders []*model.Order
	for id, s := range sentByID {
		o := AssembleOrder(s, ackedByID[id])
		if o == nil {
			continue // never delivered
		}
		orders = append(orders, o)
	}

	var phantoms []validate.ReportedFill
	for id, acks := range ackedByID {
		if _, ok := sentByID[id]; ok {
			continue
		}
		for _, a := range acks {
			if isFill(a.ExecType, a.FillQty) {
				phantoms = append(phantoms, validate.ReportedFill{
					OrderID: id, Qty: a.FillQty, Price: int64(a.FillPrice),
				})
			}
		}
	}
	return orders, phantoms
}

// preferOrig performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func preferOrig(sentOrig string, acks []topics.OrderAckedEvent) string {
	if sentOrig != "" {
		return sentOrig
	}
	return firstOrig(acks)
}

// firstOrig performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func firstOrig(acks []topics.OrderAckedEvent) string {
	for _, a := range acks {
		if a.OrigOrderID != "" {
			return a.OrigOrderID
		}
	}
	return ""
}

// responses performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func responses(acks []topics.OrderAckedEvent) []model.Response {
	out := make([]model.Response, 0, len(acks))
	for _, a := range acks {
		out = append(out, model.Response{
			ExecType:  a.ExecType,
			FillQty:   a.FillQty,
			FillPrice: a.FillPrice,
			T7Ns:      a.T7XDPEgressNS,
		})
	}
	return out
}

// Counts groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type Counts struct {
	SentEvents    uint64
	AckedEvents   uint64
	MatchedOrders uint64
}

// Run performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func Run(sents []topics.OrderSentEvent, ackeds []topics.OrderAckedEvent) (validate.Report, Counts, string) {
	orders, phantoms := Assemble(sents, ackeds)
	ordered := replay.Order(orders)
	report := validate.Run(ordered, phantoms)
	counts := Counts{
		SentEvents:    uint64(len(sents)),
		AckedEvents:   uint64(len(ackeds)),
		MatchedOrders: uint64(len(orders)),
	}
	contestant := ""
	if len(ackeds) > 0 {
		contestant = ackeds[0].ContestantID
	}
	return report, counts, contestant
}
