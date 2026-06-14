package source

import (
	"sort"

	"github.com/iicpc/correctness-validator/internal/model"
	"github.com/iicpc/correctness-validator/internal/replay"
)

// Reorderer emits orders in exactly replay.Order's total order — (EffectiveT3, Flow,
// TCPSeq), with per-connection head-of-line T3 promotion — using BOUNDED memory.
//
// Kafka partitions arrive already near-sorted (each is produced in send order ≈
// arrival order). So instead of buffering the whole session and sorting once, the
// Reorderer holds at most ~2*window orders: it accumulates, and once it has 2*window
// it promotes + sorts the buffer and releases the safe first half, carrying each
// connection's running EffectiveT3 across windows so promotion stays exact at the
// boundary. This reproduces replay.Order's output exactly as long as no order arrives
// more than `window` places out of its sorted position (far larger than real jitter).
type Reorderer struct {
	window int
	buf    []*model.Order
	// carry: per flow, the EffectiveT3 of the highest-TCPSeq order already released,
	// so the next window's promotion continues the running max instead of restarting.
	carry map[model.Flow]uint64
	emit  func(*model.Order)
}

// NewReorderer returns a Reorderer that calls emit for each order in sorted order.
func NewReorderer(window int, emit func(*model.Order)) *Reorderer {
	if window < 1 {
		window = 1
	}
	return &Reorderer{window: window, carry: make(map[model.Flow]uint64), emit: emit}
}

// Push adds one order (raw T3/Flow/TCPSeq set; EffectiveT3 will be computed) and
// releases the safe prefix once the buffer is full.
func (r *Reorderer) Push(o *model.Order) {
	r.buf = append(r.buf, o)
	if len(r.buf) >= 2*r.window {
		r.release(r.window)
	}
}

// Flush releases every remaining buffered order. Call once at end of stream.
func (r *Reorderer) Flush() {
	r.release(len(r.buf))
}

// release promotes + sorts the whole buffer, emits the first n in order, and keeps
// the rest for the next window.
func (r *Reorderer) release(n int) {
	if n <= 0 || len(r.buf) == 0 {
		return
	}
	if n > len(r.buf) {
		n = len(r.buf)
	}
	r.promote()
	sort.SliceStable(r.buf, func(i, j int) bool { return replay.Less(r.buf[i], r.buf[j]) })
	for i := 0; i < n; i++ {
		o := r.buf[i]
		// advance the per-flow running max so the next window continues from here
		if cur, ok := r.carry[o.Flow]; !ok || o.EffectiveT3 > cur {
			r.carry[o.Flow] = o.EffectiveT3
		}
		r.emit(o)
	}
	rest := make([]*model.Order, len(r.buf)-n)
	copy(rest, r.buf[n:])
	r.buf = rest
}

// promote recomputes EffectiveT3 for every buffered order exactly as replay.Order
// does within a flow (sort by TCP seq, running max of T3), but seeded from the carry
// so a flow split across windows keeps its monotone EffectiveT3.
func (r *Reorderer) promote() {
	byFlow := make(map[model.Flow][]*model.Order)
	for _, o := range r.buf {
		byFlow[o.Flow] = append(byFlow[o.Flow], o)
	}
	for flow, orders := range byFlow {
		sort.SliceStable(orders, func(i, j int) bool { return seqLessLocal(orders[i].TCPSeq, orders[j].TCPSeq) })
		prev, started := r.carry[flow] // value, ok: a prior window already released this flow
		for _, o := range orders {
			if !started || o.T3Ns > prev {
				o.EffectiveT3 = o.T3Ns
			} else {
				o.EffectiveT3 = prev // promoted to predecessor's (or carried) delivery time
			}
			prev = o.EffectiveT3
			started = true
		}
	}
}

// seqLessLocal mirrors replay's TCP-sequence comparison (wraparound-safe).
func seqLessLocal(a, b uint32) bool { return int32(a-b) < 0 }
