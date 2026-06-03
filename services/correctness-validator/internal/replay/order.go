// Package replay computes the canonical "effective delivery order" — the order in
// which TCP would have handed messages to the contestant's userspace — and is the
// deterministic processing order for the reference matching engine.
//
// Two layers (architecture_v2):
//  1. Within a flow: order by tcp_seq (TCP's definitional in-order delivery).
//  2. effective_t3: a reordered segment is promoted to its predecessor's delivery
//     time (running max over tcp_seq), since TCP would have buffered it until the
//     predecessor arrived. Global order = sort by (effective_t3, flow_id, tcp_seq).
package replay

import (
	"sort"

	"github.com/iicpc/correctness-validator/internal/model"
)

// TieToleranceNs: two orders on DIFFERENT flows whose effective_t3 differ by less
// than this are a tie — the contestant may process them in either order without it
// counting as an ordering violation (below the eBPF timestamp jitter floor).
const TieToleranceNs = 100

// Order computes effective_t3 in place and returns the orders in canonical
// delivery order. Deterministic and reproducible across runs.
func Order(orders []*model.Order) []*model.Order {
	// 1. Per flow, walk tcp_seq ascending and compute the running-max effective_t3.
	byFlow := make(map[model.Flow][]*model.Order)
	for _, o := range orders {
		byFlow[o.Flow] = append(byFlow[o.Flow], o)
	}
	for _, flowOrders := range byFlow {
		sort.Slice(flowOrders, func(i, j int) bool {
			return seqLess(flowOrders[i].TCPSeq, flowOrders[j].TCPSeq)
		})
		var prev uint64
		started := false
		for _, o := range flowOrders {
			if !started || o.T3Ns > prev {
				o.EffectiveT3 = o.T3Ns
			} else {
				o.EffectiveT3 = prev // promoted: predecessor's delivery time
			}
			prev = o.EffectiveT3
			started = true
		}
	}

	// 2. Global stable sort by (effective_t3, flow_id, tcp_seq).
	out := make([]*model.Order, len(orders))
	copy(out, orders)
	sort.SliceStable(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if a.EffectiveT3 != b.EffectiveT3 {
			return a.EffectiveT3 < b.EffectiveT3
		}
		if a.Flow != b.Flow {
			return a.Flow.Less(b.Flow)
		}
		return seqLess(a.TCPSeq, b.TCPSeq)
	})
	return out
}

// CrossFlowTie reports whether two orders are a cross-flow tie within the 100ns
// tolerance — used by violation detection to suppress order-dependent violations
// the contestant was free to resolve either way. Within a flow there is no
// tolerance (the byte stream is unambiguous).
func CrossFlowTie(a, b *model.Order) bool {
	if a.Flow == b.Flow {
		return false
	}
	return absDiff(a.EffectiveT3, b.EffectiveT3) < TieToleranceNs
}

// seqLess compares TCP sequence numbers with wrap tolerance (within a flow the
// span is far below 2^31, so signed-delta ordering is correct).
func seqLess(a, b uint32) bool {
	return int32(a-b) < 0
}

func absDiff(a, b uint64) uint64 {
	if a > b {
		return a - b
	}
	return b - a
}
