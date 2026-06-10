// Package pipeline ties the per-session steps together: assemble the joined
// order log from the raw sent/acked events, order it by effective delivery time,
// and validate the reported fills against the reference engine. Pure and
// deterministic — the Kafka drain, Postgres persistence, and scores.correctness
// publish live in the surrounding I/O packages.
package pipeline

import (
	"github.com/iicpc/correctness-validator/internal/model"
	"github.com/iicpc/correctness-validator/internal/replay"
	"github.com/iicpc/correctness-validator/internal/validate"
	"github.com/iicpc/schemas/topics"
)

func isFill(execType string, qty uint64) bool {
	return qty > 0 && (execType == "1" || execType == "2" || execType == "F")
}

// Assemble joins orders.sent (request facts) with orders.acked (flow/t3/responses)
// per order_id. Orders with no acked event are excluded — they were never
// delivered to the contestant's userspace, so a correct engine never saw them.
// Acked fills whose order_id was never sent become phantom inputs.
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
		acks := ackedByID[id]
		if len(acks) == 0 {
			continue // never delivered
		}
		first := acks[0]
		o := &model.Order{
			OrderID: id,
			Flow:    model.Flow{SrcIP: first.SrcIP, SrcPort: first.SrcPort},
			TCPSeq:  first.TCPSeq,
			T3Ns:    first.T3XDPIngressNS,
			Side:    model.SideFrom(s.Side),
			// orders.sent.price is a raw integer (the bot writes FIX tag 44=<int>),
			// but orders.acked.fill_price is fixed-point scaled by TelemetryPriceScale
			// (eBPF parses the contestant's decimal ×1e9). Lift the reference order
			// into the same scaled domain so the reference fills it produces are
			// directly comparable to the reported fill_price; otherwise every fill is
			// a phantom price violation and CorrectnessScore collapses to ~0.
			Price: int64(s.Price) * int64(topics.TelemetryPriceScale),
			Qty:   s.Qty,
			Kind:  model.KindFrom(s.PayloadType, s.OrdType),
			// Prefer the bot-authoritative orig_order_id from orders.sent; fall back
			// to the contestant's echoed tag 41 (acked) only when the bot did not
			// supply one (transitional). Keying off the sent value means a contestant
			// cannot steer the reference engine by omitting/altering the cancel target.
			OrigOrderID: preferOrig(s.OrigOrderID, acks),
			Responses:   responses(acks),
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

// preferOrig returns the bot-authoritative orig_order_id from orders.sent when
// present, falling back to the contestant's echoed tag 41 (orders.acked) only when
// the bot supplied none (transitional). The reference engine must not be steerable
// by contestant-controlled cancel targets.
func preferOrig(sentOrig string, acks []topics.OrderAckedEvent) string {
	if sentOrig != "" {
		return sentOrig
	}
	return firstOrig(acks)
}

func firstOrig(acks []topics.OrderAckedEvent) string {
	for _, a := range acks {
		if a.OrigOrderID != "" {
			return a.OrigOrderID
		}
	}
	return ""
}

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

// Counts is the telemetry volume behind one session's verdict: how many
// orders.sent events the drain returned, how many orders.acked events survived
// dedup, and how many distinct orders appeared in BOTH streams — the replay's
// actual inputs. The gap between SentEvents and MatchedOrders is the
// completeness signal: a lost acked event silently excludes its order from the
// replay, and a lost sent flush turns its acks into false phantoms, so the
// counters travel with the verdict (summary row + CorrectnessScoreEvent) for
// score-computer's coverage gate.
type Counts struct {
	SentEvents    uint64
	AckedEvents   uint64
	MatchedOrders uint64
}

// Run is the deterministic core: assemble -> order by effective_t3 -> validate.
// Returns the report, the completeness counts, and the contestant_id (from the
// acked events). The caller stamps computed_at and builds/publishes the
// CorrectnessScoreEvent.
func Run(sents []topics.OrderSentEvent, ackeds []topics.OrderAckedEvent) (validate.Report, Counts, string) {
	orders, phantoms := Assemble(sents, ackeds)
	ordered := replay.Order(orders)
	report := validate.Run(ordered, phantoms)
	// MatchedOrders is len(orders) by construction: Assemble keeps exactly the
	// distinct sent order_ids that have at least one acked event.
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
