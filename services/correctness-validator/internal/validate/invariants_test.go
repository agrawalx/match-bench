package validate

import (
	"testing"

	"github.com/iicpc/correctness-validator/internal/model"
)

// TestCrossFlowPredicate_WorkedExamples pins down the W-window predicate with the
// exact numbers from the task spec.
func TestCrossFlowPredicate_WorkedExamples(t *testing.T) {
	// A arrives at 1000us, B arrives at 1300us, W=500us. B processed first (caller
	// already knows this). gap=300<=500 -> inversion, NOT a violation, jitter=300us.
	isInv, violation, jitterNs := CrossFlowPredicate(1000*1000, 1300*1000, 500)
	if !isInv || violation || jitterNs != 300*1000 {
		t.Fatalf("example 1: got isInv=%v violation=%v jitterNs=%d, want isInv=true violation=false jitterNs=300000",
			isInv, violation, jitterNs)
	}

	// A arrives at 1000us, B at 1800us, W=500us. gap=800>500 -> violation, jitter=800us.
	isInv, violation, jitterNs = CrossFlowPredicate(1000*1000, 1800*1000, 500)
	if !isInv || !violation || jitterNs != 800*1000 {
		t.Fatalf("example 2: got isInv=%v violation=%v jitterNs=%d, want isInv=true violation=true jitterNs=800000",
			isInv, violation, jitterNs)
	}

	// A arrives at 1000us, B arrives at 900us (B arrived FIRST). Not an inversion —
	// arrival order and processing order agree.
	isInv, violation, jitterNs = CrossFlowPredicate(1000*1000, 900*1000, 500)
	if isInv || violation || jitterNs != 0 {
		t.Fatalf("example 3: got isInv=%v violation=%v jitterNs=%d, want all false/zero", isInv, violation, jitterNs)
	}
}

// TestCrossFlowPredicate_ExactlyAtWindow: gap == W is NOT a violation ("<=" tolerance).
func TestCrossFlowPredicate_ExactlyAtWindow(t *testing.T) {
	isInv, violation, jitterNs := CrossFlowPredicate(1000, 1500, 500) // ns scale, gap=500ns... use us properly below
	_ = isInv
	_ = violation
	_ = jitterNs
	isInv, violation, jitterNs = CrossFlowPredicate(1_000_000, 1_500_000, 500) // gap exactly 500us
	if !isInv || violation {
		t.Fatalf("gap==W must not violate: isInv=%v violation=%v jitterNs=%d", isInv, violation, jitterNs)
	}
}

func flowA() model.Flow { return model.Flow{SrcIP: 1, SrcPort: 1} }
func flowB() model.Flow { return model.Flow{SrcIP: 2, SrcPort: 2} }

func invOrd(id string, kind model.Kind, flow model.Flow, tcpSeq uint32, t3 uint64, qty uint64, resp ...model.Response) *model.Order {
	return &model.Order{OrderID: id, Kind: kind, Flow: flow, TCPSeq: tcpSeq, T3Ns: t3, Qty: qty, Responses: resp}
}

func respAt(qty, price, t7 uint64) model.Response {
	return model.Response{ExecType: "2", FillQty: qty, FillPrice: price, T7Ns: t7}
}

func TestInvariants_OverfillDetected(t *testing.T) {
	v := NewInvariantsValidator(500)
	v.Apply(invOrd("O1", model.NewLimit, flowA(), 1, 1000, 10, respAt(6, 100, 2000), respAt(6, 100, 3000)))
	r := v.Finish()
	if r.Overfills != 1 {
		t.Fatalf("expected 1 overfill, got %+v", r)
	}
}

func TestInvariants_LostOrderCounted(t *testing.T) {
	v := NewInvariantsValidator(500)
	v.Apply(invOrd("O1", model.NewLimit, flowA(), 1, 1000, 10)) // no responses
	r := v.Finish()
	if r.LostOrders != 1 {
		t.Fatalf("expected 1 lost order, got %+v", r)
	}
}

func TestInvariants_LostCancelCounted(t *testing.T) {
	v := NewInvariantsValidator(500)
	v.Apply(invOrd("C1", model.Cancel, flowA(), 1, 1000, 0)) // no responses
	r := v.Finish()
	if r.LostCancels != 1 {
		t.Fatalf("expected 1 lost cancel, got %+v", r)
	}
}

func TestInvariants_PerFlowFIFOBreachFlagged(t *testing.T) {
	v := NewInvariantsValidator(500)
	// Same flow, TCPSeq 1 then 2, but seq-2's order is processed (min T7) before seq-1's.
	v.Apply(invOrd("O1", model.NewLimit, flowA(), 1, 1000, 10, respAt(10, 100, 5000)))
	v.Apply(invOrd("O2", model.NewLimit, flowA(), 2, 1500, 10, respAt(10, 100, 2000)))
	r := v.Finish()
	if r.TimeViolations == 0 {
		t.Fatalf("expected a per-flow FIFO breach, got %+v", r)
	}
}

func TestInvariants_PerFlowFIFOCleanNotFlagged(t *testing.T) {
	v := NewInvariantsValidator(500)
	v.Apply(invOrd("O1", model.NewLimit, flowA(), 1, 1000, 10, respAt(10, 100, 2000)))
	v.Apply(invOrd("O2", model.NewLimit, flowA(), 2, 1500, 10, respAt(10, 100, 5000)))
	r := v.Finish()
	if r.TimeViolations != 0 {
		t.Fatalf("clean FIFO must not flag, got %+v", r)
	}
}

func TestInvariants_CrossFlowWithinWindowNotViolation(t *testing.T) {
	v := NewInvariantsValidator(500) // W=500us
	// A arrives at t3=1000us, processed (T7) at 10000ns.
	v.Apply(invOrd("A", model.NewLimit, flowA(), 1, 1000*1000, 10, respAt(10, 100, 10_000_000)))
	// B arrives at t3=1300us (gap 300us <= W), processed BEFORE A (T7 smaller).
	v.Apply(invOrd("B", model.NewLimit, flowB(), 1, 1300*1000, 10, respAt(10, 100, 5_000_000)))
	r := v.Finish()
	if r.TimeViolations != 0 {
		t.Fatalf("gap within W must not be a scored violation, got %+v", r)
	}
	if r.Jitter.Count != 1 {
		t.Fatalf("expected 1 recorded inversion in the jitter histogram, got %+v", r.Jitter)
	}
}

func TestInvariants_CrossFlowBeyondWindowViolatesAndRecordsJitter(t *testing.T) {
	v := NewInvariantsValidator(500) // W=500us
	v.Apply(invOrd("A", model.NewLimit, flowA(), 1, 1000*1000, 10, respAt(10, 100, 10_000_000)))
	// B arrives at t3=1800us (gap 800us > W), processed before A.
	v.Apply(invOrd("B", model.NewLimit, flowB(), 1, 1800*1000, 10, respAt(10, 100, 5_000_000)))
	r := v.Finish()
	if r.TimeViolations != 1 {
		t.Fatalf("gap beyond W must be a scored violation, got %+v", r)
	}
	if r.Jitter.Count != 1 || r.Jitter.MaxUs != 800 {
		t.Fatalf("expected jitter recorded at 800us, got %+v", r.Jitter)
	}
}

func TestInvariants_UnmatchedResponseIsMetricOnlyNotScored(t *testing.T) {
	v := NewInvariantsValidator(500)
	v.Apply(invOrd("O1", model.NewLimit, flowA(), 1, 1000, 10, respAt(10, 100, 2000)))
	v.AddUnmatched("ghost", 5, 100)
	r := v.Finish()
	if r.PhantomFills != 1 {
		t.Fatalf("expected unmatched response counted as metric, got %+v", r)
	}
	if r.CorrectnessScore() != 1.0 {
		t.Fatalf("unmatched response must not affect score, got %v", r.CorrectnessScore())
	}
}

// TestInvariants_EndToEndSyntheticStream feeds a small synthetic sent/acked-derived
// order stream through invariants mode end-to-end and asserts on the resulting
// violations + jitter stats together.
func TestInvariants_EndToEndSyntheticStream(t *testing.T) {
	v := NewInvariantsValidator(500)

	// Clean same-flow pair: no violations.
	v.Apply(invOrd("clean1", model.NewLimit, flowA(), 1, 1000, 10, respAt(10, 100, 1_000_000)))
	v.Apply(invOrd("clean2", model.NewLimit, flowA(), 2, 2000, 10, respAt(10, 100, 2_000_000)))

	// Cross-flow, small skew (within W): inversion but not a violation.
	v.Apply(invOrd("tie1", model.NewLimit, flowA(), 3, 10_000*1000, 10, respAt(10, 100, 20_000_000)))
	v.Apply(invOrd("tie2", model.NewLimit, flowB(), 1, 10_200*1000, 10, respAt(10, 100, 15_000_000)))

	// Cross-flow, large skew (beyond W): a violation.
	v.Apply(invOrd("far1", model.NewLimit, flowA(), 4, 50_000*1000, 10, respAt(10, 100, 60_000_000)))
	v.Apply(invOrd("far2", model.NewLimit, flowB(), 2, 52_000*1000, 10, respAt(10, 100, 55_000_000)))

	// A lost order.
	v.Apply(invOrd("lost1", model.NewLimit, flowA(), 5, 99_000*1000, 10))

	// An overfill.
	v.Apply(invOrd("over1", model.NewLimit, flowB(), 3, 60_000*1000, 5, respAt(3, 100, 61_000_000), respAt(3, 100, 62_000_000)))

	r := v.Finish()
	if r.LostOrders != 1 {
		t.Fatalf("expected 1 lost order, got %+v", r)
	}
	if r.Overfills != 1 {
		t.Fatalf("expected 1 overfill, got %+v", r)
	}
	if r.TimeViolations != 1 {
		t.Fatalf("expected exactly 1 scored cross-flow time violation (far1/far2), got %+v", r)
	}
	if r.Jitter.Count != 2 {
		t.Fatalf("expected 2 recorded inversions (tie + far), got %+v", r.Jitter)
	}
	if r.Jitter.MaxUs != 2000 {
		t.Fatalf("expected max jitter 2000us (far pair), got %+v", r.Jitter)
	}
}
