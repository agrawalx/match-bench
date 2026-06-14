package validate

import (
	"testing"

	"github.com/iicpc/correctness-validator/internal/model"
)

// runStream drives the StreamValidator with the same already-ordered orders that
// validate.Run consumes, plus phantoms, and returns the report.
func runStream(ordered []*model.Order, phantoms []ReportedFill) Report {
	v := NewStreamValidator()
	for _, o := range ordered {
		v.Apply(o)
	}
	for _, pf := range phantoms {
		v.AddPhantom(pf)
	}
	return v.Finish()
}

// assertScoreEquiv asserts the streaming validator produces the SAME score as the
// batch validator: identical TotalFills/ValidFills/PhantomFills/Overfills/SelfTrades,
// identical total ViolationCount, and identical CorrectnessScore. Streaming may
// reclassify a *late-contested* queue jump between Time and Price (both invalid), so
// those two are compared as a SUM, not individually — the score is unaffected either way.
func assertScoreEquiv(t *testing.T, name string, ordered []*model.Order, phantoms []ReportedFill) {
	t.Helper()
	b := Run(ordered, phantoms)
	s := runStream(ordered, phantoms)
	bad := b.TotalFills != s.TotalFills ||
		b.ValidFills != s.ValidFills ||
		b.PhantomFills != s.PhantomFills ||
		b.Overfills != s.Overfills ||
		b.SelfTrades != s.SelfTrades ||
		(b.TimeViolations+b.PriceViolations) != (s.TimeViolations+s.PriceViolations) ||
		b.ViolationCount() != s.ViolationCount() ||
		b.CorrectnessScore() != s.CorrectnessScore()
	if bad {
		t.Fatalf("%s: score mismatch\n  batch =%+v score=%.6f\n  stream=%+v score=%.6f",
			name, b, b.CorrectnessScore(), s, s.CorrectnessScore())
	}
}

func TestStreamEquivCleanCross(t *testing.T) {
	assertScoreEquiv(t, "clean", []*model.Order{
		order("S1", model.NewLimit, model.Sell, 100, 10, fill(10, 100)),
		order("B1", model.NewLimit, model.Buy, 100, 10, fill(10, 100)),
	}, nil)
}

func TestStreamEquivOverfill(t *testing.T) {
	assertScoreEquiv(t, "overfill", []*model.Order{
		order("S1", model.NewLimit, model.Sell, 100, 20),
		order("B1", model.NewLimit, model.Buy, 100, 10, fill(6, 100), fill(6, 100)),
	}, nil)
}

func TestStreamEquivWrongPrice(t *testing.T) {
	assertScoreEquiv(t, "wrongprice", []*model.Order{
		order("S1", model.NewLimit, model.Sell, 100, 10),
		order("B1", model.NewLimit, model.Buy, 100, 10, fill(10, 101)),
	}, nil)
}

func TestStreamEquivBeyondRefQty(t *testing.T) {
	assertScoreEquiv(t, "beyondref", []*model.Order{
		order("S1", model.NewLimit, model.Sell, 100, 10),
		order("B1", model.NewLimit, model.Buy, 100, 20, fill(15, 100)),
	}, nil)
}

func TestStreamEquivUnderReport(t *testing.T) {
	assertScoreEquiv(t, "underreport", []*model.Order{
		order("S1", model.NewLimit, model.Sell, 100, 10),
		order("B1", model.NewLimit, model.Buy, 100, 10, fill(5, 100)),
	}, nil)
}

func TestStreamEquivPhantom(t *testing.T) {
	assertScoreEquiv(t, "phantom", nil, []ReportedFill{{OrderID: "ghost", Qty: 5, Price: 100}})
}

// Time-priority: S1 and S2 both rest at 100; B1 crosses and the reference fills S1
// (FIFO). The contestant instead reports S2 filled — a queue jump. Batch labels it
// Time (S1 has a lower seq); streaming may label it Price (S1 already left the book
// by S2's finalize). Either way the fill is INVALID, so the score must match.
func TestStreamEquivTimePriority(t *testing.T) {
	assertScoreEquiv(t, "timepriority", []*model.Order{
		order("S1", model.NewLimit, model.Sell, 100, 10),
		order("S2", model.NewLimit, model.Sell, 100, 10, fill(10, 100)),
		order("B1", model.NewLimit, model.Buy, 100, 10),
	}, nil)
}

// Self-trade: same participant (bot id 7) on both sides crossing at the same price.
func TestStreamEquivSelfTrade(t *testing.T) {
	assertScoreEquiv(t, "selftrade", []*model.Order{
		order("S_7_1_O", model.NewLimit, model.Sell, 100, 10),
		order("B_7_2_O", model.NewLimit, model.Buy, 100, 10, fill(10, 100)),
	}, nil)
}

// Cancel + replace churn: orders leave the book mid-session (exercises eviction +
// finalize-on-exit + Forget), with a phantom for good measure.
func TestStreamEquivChurnMixed(t *testing.T) {
	assertScoreEquiv(t, "churn", []*model.Order{
		order("S1", model.NewLimit, model.Sell, 101, 10),
		order("S2", model.NewLimit, model.Sell, 100, 20),
		order("C1", model.Cancel, model.Sell, 0, 0), // cancels S1 (OrigOrderID set below)
		order("B1", model.NewLimit, model.Buy, 100, 10, fill(10, 100)),
		order("B2", model.NewLimit, model.Buy, 100, 10, fill(10, 100)),
		order("S3", model.NewLimit, model.Sell, 100, 5),
	}, []ReportedFill{{OrderID: "ghost", Qty: 1, Price: 50}})
}
