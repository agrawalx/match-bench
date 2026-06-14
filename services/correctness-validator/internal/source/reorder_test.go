package source

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/iicpc/correctness-validator/internal/model"
	"github.com/iicpc/correctness-validator/internal/replay"
)

func cloneOrder(o *model.Order) *model.Order {
	c := *o
	c.EffectiveT3 = 0 // recomputed by both paths
	return &c
}

func cloneOrders(in []*model.Order) []*model.Order {
	out := make([]*model.Order, len(in))
	for i, o := range in {
		out[i] = cloneOrder(o)
	}
	return out
}

// genArrivalOrders builds orders in approximate arrival order: T3 mostly increasing
// (with occasional small inversions), each flow's TCP seq strictly increasing — i.e.
// exactly the near-sorted, per-connection-ordered shape Kafka delivers.
func genArrivalOrders(rng *rand.Rand, n, flows int) []*model.Order {
	orders := make([]*model.Order, n)
	seqByFlow := make(map[int]uint32)
	t3 := uint64(1_000_000)
	for i := range orders {
		f := rng.Intn(flows)
		seqByFlow[f] += uint32(1 + rng.Intn(4))
		t3 += uint64(rng.Intn(6))
		eventT3 := t3
		if rng.Intn(8) == 0 && eventT3 > 3 { // occasional small inversion vs send order
			eventT3 -= uint64(rng.Intn(3))
		}
		orders[i] = &model.Order{
			OrderID: fmt.Sprintf("o%05d", i),
			Flow:    model.Flow{SrcIP: 0x0a000001, SrcPort: uint16(1 + f)},
			TCPSeq:  seqByFlow[f],
			T3Ns:    eventT3,
		}
	}
	return orders
}

// TestReordererMatchesReplayOrder proves the bounded windowed Reorderer emits orders
// in EXACTLY the order (and with exactly the EffectiveT3) that replay.Order produces
// for the whole session — so the streaming validator replays orders identically to
// the batch one and cannot compute a different score.
func TestReordererMatchesReplayOrder(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	for trial := 0; trial < 500; trial++ {
		n := 1 + rng.Intn(600)
		flows := 1 + rng.Intn(8)
		arrival := genArrivalOrders(rng, n, flows)

		want := replay.Order(cloneOrders(arrival)) // batch oracle

		window := 8 + rng.Intn(120)
		var got []*model.Order
		r := NewReorderer(window, func(o *model.Order) { got = append(got, o) })
		for _, o := range cloneOrders(arrival) {
			r.Push(o)
		}
		r.Flush()

		if len(got) != len(want) {
			t.Fatalf("trial %d: got %d orders, want %d", trial, len(got), len(want))
		}
		for i := range want {
			if got[i].OrderID != want[i].OrderID || got[i].EffectiveT3 != want[i].EffectiveT3 {
				t.Fatalf("trial %d (n=%d flows=%d window=%d) pos %d:\n  got  %s eff=%d\n  want %s eff=%d",
					trial, n, flows, window, i, got[i].OrderID, got[i].EffectiveT3, want[i].OrderID, want[i].EffectiveT3)
			}
		}
	}
}

// TestReordererFlushOnly confirms a buffer smaller than the window still drains fully
// in correct order on Flush.
func TestReordererFlushOnly(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	arrival := genArrivalOrders(rng, 25, 3)
	want := replay.Order(cloneOrders(arrival))
	var got []*model.Order
	r := NewReorderer(1000, func(o *model.Order) { got = append(got, o) }) // window >> n
	for _, o := range cloneOrders(arrival) {
		r.Push(o)
	}
	r.Flush()
	if len(got) != len(want) {
		t.Fatalf("got %d want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].OrderID != want[i].OrderID || got[i].EffectiveT3 != want[i].EffectiveT3 {
			t.Fatalf("pos %d: got %s/%d want %s/%d", i, got[i].OrderID, got[i].EffectiveT3, want[i].OrderID, want[i].EffectiveT3)
		}
	}
}
