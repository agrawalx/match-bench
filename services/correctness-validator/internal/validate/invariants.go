// Pass-2 book-free invariants mode (docs/multi-contestant-audit.md §5, P-F pass 2 /
// P-G). No reference matching engine: checks are per-order/per-flow accounting plus a
// cross-flow processing-order comparison against arrival time (t3), replacing the
// full reference-book replay that pass 1 (single-connection, invariants.go's sibling
// batch/stream validators) already owns.
//
// Memory during ingestion is O(inflight): one lightweight record per live order (no
// book, no price levels). The cross-flow inversion sweep and jitter histogram are
// computed once at Finish() over the accumulated per-order records — O(n log n) to
// sort by processing order, O(k) pairwise inversion checks where k is the number of
// actual inversions found (cheap in practice: a compliant engine has ~zero). This is
// a deliberate simplification versus a fully bounded sliding-window streaming sweep;
// see the package-level note in the correctness-validator design doc.
package validate

import (
	"sort"

	"github.com/iicpc/correctness-validator/internal/model"
)

// DefaultCrossFlowWindowUs is the default W for the cross-flow priority-vs-t3 check
// (env CROSS_FLOW_WINDOW_US).
const DefaultCrossFlowWindowUs = 500

// CrossFlowPredicate is the P-F/P-G W-window predicate, pinned down precisely:
//
// Let A and B be orders on different flows. t3(X) is X's kernel-capture arrival
// time. "B processed before A" means B's first response (min T7) occurred before
// A's. An INVERSION is: A arrived first (t3(A) < t3(B)) but B was processed first.
//
// For an inversion, jitter is ALWAYS recorded (for the P-G histogram) as the
// magnitude t3(B) - t3(A), regardless of whether it trips the window. It is a
// SCORED violation only when that magnitude exceeds the window W: the reordering is
// then too large to be explained by ordinary cross-flow skew (per-flow FIFO already
// covers same-flow ordering; W is the forgiveness for real skew across independent
// TCP flows).
//
// Worked examples (µs), pinned by TestCrossFlowPredicate_WorkedExamples:
//   - t3(A)=1000, t3(B)=1300, W=500: inversion, gap=300<=500 -> NOT a violation,
//     jitter=300 IS recorded.
//   - t3(A)=1000, t3(B)=1800, W=500: inversion, gap=800>500 -> violation AND
//     jitter=800 recorded.
//   - t3(A)=1000, t3(B)=900 (B arrived first): NOT an inversion (arrival order and
//     processing order agree) -> no violation, no jitter.
//
// isInversion tells the caller whether A arrived before B (the precondition for
// this predicate to apply at all — the caller must independently know B was
// processed before A for this pair to be considered).
func CrossFlowPredicate(t3ANs, t3BNs uint64, windowUs uint64) (isInversion bool, violation bool, jitterNs uint64) {
	if t3ANs >= t3BNs {
		return false, false, 0
	}
	jitterNs = t3BNs - t3ANs
	violation = jitterNs > windowUs*1000
	return true, violation, jitterNs
}

// JitterStats summarizes the P-G inversion-magnitude distribution for one session.
type JitterStats struct {
	Count         uint64
	InversionRate float64 // inversions / total processed orders
	P50Us         float64
	P99Us         float64
	P999Us        float64
	MaxUs         float64
}

// jitterHistogram accumulates inversion magnitudes (ns) for one session and derives
// percentiles on demand. A plain sorted-slice percentile calculator rather than a
// true HDR histogram — adequate at per-session scale and exact, at the cost of O(n
// log n) at Finish() instead of O(1) per sample; acceptable because in practice
// inversions are rare for a compliant engine.
type jitterHistogram struct {
	samplesNs []uint64
}

func (h *jitterHistogram) record(ns uint64) { h.samplesNs = append(h.samplesNs, ns) }

func (h *jitterHistogram) stats(totalProcessed uint64) JitterStats {
	n := len(h.samplesNs)
	if n == 0 {
		return JitterStats{}
	}
	sorted := make([]uint64, n)
	copy(sorted, h.samplesNs)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	pct := func(p float64) float64 {
		idx := int(p * float64(n-1))
		if idx < 0 {
			idx = 0
		}
		if idx >= n {
			idx = n - 1
		}
		return float64(sorted[idx]) / 1000.0 // ns -> us
	}
	rate := 0.0
	if totalProcessed > 0 {
		rate = float64(n) / float64(totalProcessed)
	}
	return JitterStats{
		Count:         uint64(n),
		InversionRate: rate,
		P50Us:         pct(0.50),
		P99Us:         pct(0.99),
		P999Us:        pct(0.999),
		MaxUs:         float64(sorted[n-1]) / 1000.0,
	}
}

// invOrder is the lightweight per-order record kept for invariants mode — no book
// state, just what's needed for the accounting + cross-flow checks.
type invOrder struct {
	orderID string
	kind    model.Kind
	flow    model.Flow
	tcpSeq  uint32
	t3Ns    uint64
	qty     uint64
	minT7Ns uint64
	hasResp bool
}

// InvariantsValidator implements the pass-2 book-free checks. Feed it every order (in
// any order — arrival/EffectiveT3 order from the existing Reorderer is fine) via
// Apply, then call Finish for the report.
type InvariantsValidator struct {
	windowUs uint64
	orders   map[string]*invOrder
	rep      Report
}

// NewInvariantsValidator constructs an invariants-mode validator with cross-flow
// window W (env CROSS_FLOW_WINDOW_US, default DefaultCrossFlowWindowUs).
func NewInvariantsValidator(windowUs uint64) *InvariantsValidator {
	if windowUs == 0 {
		windowUs = DefaultCrossFlowWindowUs
	}
	return &InvariantsValidator{windowUs: windowUs, orders: make(map[string]*invOrder)}
}

// Apply registers one order's accounting: overfill (own qty only — book-free) is
// checked immediately; per-flow FIFO and cross-flow priority are checked at Finish,
// once every order's minT7 is known.
func (v *InvariantsValidator) Apply(o *model.Order) {
	rec := &invOrder{
		orderID: o.OrderID,
		kind:    o.Kind,
		flow:    o.Flow,
		tcpSeq:  o.TCPSeq,
		t3Ns:    o.T3Ns,
		qty:     o.Qty,
	}
	var cumFilled uint64
	for _, resp := range o.Responses {
		if !isFill(resp.ExecType, resp.FillQty) {
			continue
		}
		v.rep.TotalFills++
		cumFilled += resp.FillQty
		if !rec.hasResp || resp.T7Ns < rec.minT7Ns {
			rec.minT7Ns = resp.T7Ns
			rec.hasResp = true
		}
		if cumFilled > o.Qty {
			v.rep.Overfills++
			v.rep.add(Overfill, o.OrderID, resp.FillQty, int64(resp.FillPrice),
				"cumulative reported fill qty exceeds order qty (book-free own-qty check)")
		} else {
			v.rep.ValidFills++
		}
	}
	if !rec.hasResp {
		for _, resp := range o.Responses {
			if resp.T7Ns != 0 {
				rec.hasResp = true
				rec.minT7Ns = resp.T7Ns
				break
			}
		}
	}
	v.orders[o.OrderID] = rec
}

// AddUnmatched records a response reported for an order_id never sent. Kept as the
// unmatched_responses metric only (docs/multi-contestant-audit.md §5: phantom-fill
// is removed from scoring in both modes).
func (v *InvariantsValidator) AddUnmatched(_ string, _ uint64, _ int64) {
	v.rep.PhantomFills++
}

// Finish runs the two checks that need the full order set (per-flow FIFO, lost
// order/cancel accounting, cross-flow priority vs t3) and returns the report.
func (v *InvariantsValidator) Finish() Report {
	responded := make([]*invOrder, 0, len(v.orders))
	for _, rec := range v.orders {
		if rec.hasResp {
			responded = append(responded, rec)
		} else {
			v.rep.add(lostViolationType(rec.kind), rec.orderID, 0, 0,
				"order/cancel sent but never received any response")
			if rec.kind == model.Cancel {
				v.rep.LostCancels++
			} else {
				v.rep.LostOrders++
			}
		}
	}

	// Per-flow FIFO: process in min-T7 order; within a flow, TCPSeq must be
	// non-decreasing. A "Time violation" is out-of-order TCPSeq within one flow.
	sort.Slice(responded, func(i, j int) bool { return responded[i].minT7Ns < responded[j].minT7Ns })
	lastSeq := make(map[model.Flow]uint32)
	lastSeqSet := make(map[model.Flow]bool)
	for _, rec := range responded {
		if lastSeqSet[rec.flow] && seqLE(rec.tcpSeq, lastSeq[rec.flow]) {
			v.rep.TimeViolations++
			v.rep.add(Time, rec.orderID, 0, 0, "out-of-order TCPSeq within one flow")
		}
		lastSeq[rec.flow] = rec.tcpSeq
		lastSeqSet[rec.flow] = true
	}

	// Cross-flow priority vs t3, within window W. Single pass in processing
	// (min-T7) order: each order contributes at most ONE jitter sample — the gap to
	// the latest-arriving cross-flow order processed before it (a running max of
	// t3, with a second-best from a different flow so a same-flow max never
	// masks a cross-flow jump). Per-victim sampling keeps this O(n log n) overall
	// — a pairwise enumeration is quadratic in both time and sample count and
	// cannot survive pass-2 volumes — and reads as "how far was this order
	// jumped", which is the jitter definition in docs/multi-contestant-audit.md §5.
	jitter := &jitterHistogram{}
	var max1T3, max2T3 uint64
	var max1Flow model.Flow
	var max1Set, max2Set bool
	for _, rec := range responded {
		candT3, candSet := max1T3, max1Set
		if max1Set && rec.flow == max1Flow {
			candT3, candSet = max2T3, max2Set
		}
		if candSet {
			isInv, violation, jitterNs := CrossFlowPredicate(rec.t3Ns, candT3, v.windowUs)
			if isInv {
				jitter.record(jitterNs)
				if violation {
					v.rep.TimeViolations++
					v.rep.add(Time, rec.orderID, 0, 0, "cross-flow processed after a later-arriving order beyond window W")
				}
			}
		}
		if !max1Set || rec.t3Ns > max1T3 {
			if max1Set && max1Flow != rec.flow && (!max2Set || max1T3 > max2T3) {
				max2T3, max2Set = max1T3, true
			}
			max1T3, max1Flow, max1Set = rec.t3Ns, rec.flow, true
		} else if rec.flow != max1Flow && (!max2Set || rec.t3Ns > max2T3) {
			max2T3, max2Set = rec.t3Ns, true
		}
	}
	v.rep.Jitter = jitter.stats(uint64(len(responded)))
	return v.rep
}

func lostViolationType(k model.Kind) ViolationType {
	if k == model.Cancel {
		return LostCancel
	}
	return LostOrder
}

// seqLE reports whether a <= b using wraparound-safe TCP sequence comparison
// (mirrors source.seqLessLocal / replay's TCPSeq handling).
func seqLE(a, b uint32) bool {
	return a == b || int32(a-b) < 0
}
