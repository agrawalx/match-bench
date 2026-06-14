// Package replay implements order behavior.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package replay

import (
	"sort"

	"github.com/iicpc/correctness-validator/internal/model"
)

const TieToleranceNs = 100

// Order performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func Order(orders []*model.Order) []*model.Order {
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
				o.EffectiveT3 = prev
			}
			prev = o.EffectiveT3
			started = true
		}
	}

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

// Less reports whether a sorts before b under the SAME total order Order() applies
// (EffectiveT3, then flow, then TCP seq). The streaming source's reorder heap uses
// this so it releases orders in exactly the order the batch path replays them.
// EffectiveT3 must already be populated on both orders.
func Less(a, b *model.Order) bool {
	if a.EffectiveT3 != b.EffectiveT3 {
		return a.EffectiveT3 < b.EffectiveT3
	}
	if a.Flow != b.Flow {
		return a.Flow.Less(b.Flow)
	}
	return seqLess(a.TCPSeq, b.TCPSeq)
}

// CrossFlowTie performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func CrossFlowTie(a, b *model.Order) bool {
	if a.Flow == b.Flow {
		return false
	}
	return absDiff(a.EffectiveT3, b.EffectiveT3) < TieToleranceNs
}

// seqLess performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func seqLess(a, b uint32) bool {
	return int32(a-b) < 0
}

// absDiff performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func absDiff(a, b uint64) uint64 {
	if a > b {
		return a - b
	}
	return b - a
}
