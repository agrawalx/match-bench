package validate

import (
	"reflect"
	"testing"

	"github.com/iicpc/correctness-validator/internal/model"
)

func fill(qty, price uint64) model.Response {
	return model.Response{ExecType: "2", FillQty: qty, FillPrice: price}
}

func order(id string, kind model.Kind, side model.Side, price int64, qty uint64, resp ...model.Response) *model.Order {
	return &model.Order{OrderID: id, Kind: kind, Side: side, Price: price, Qty: qty, Responses: resp}
}

func TestCleanCrossIsAllValid(t *testing.T) {
	// S1 rests, B1 crosses; both correctly report a 10@100 fill.
	ordered := []*model.Order{
		order("S1", model.NewLimit, model.Sell, 100, 10, fill(10, 100)),
		order("B1", model.NewLimit, model.Buy, 100, 10, fill(10, 100)),
	}
	r := Run(ordered, nil)
	if r.TotalFills != 2 || r.ValidFills != 2 {
		t.Fatalf("expected 2/2 valid, got %+v", r)
	}
	if r.CorrectnessScore() != 1.0 || len(r.Violations) != 0 {
		t.Fatalf("expected score 1.0 no violations, got %+v", r)
	}
}

func TestOverfillFlagged(t *testing.T) {
	// Maker has plenty; buyer (qty 10) over-reports 6+6 = 12.
	ordered := []*model.Order{
		order("S1", model.NewLimit, model.Sell, 100, 20),
		order("B1", model.NewLimit, model.Buy, 100, 10, fill(6, 100), fill(6, 100)),
	}
	r := Run(ordered, nil)
	if r.Overfills != 1 {
		t.Fatalf("expected 1 overfill, got %+v", r)
	}
	if r.TotalFills != 2 || r.ValidFills != 1 {
		t.Fatalf("expected 2 total / 1 valid, got %+v", r)
	}
}

func TestPhantomFlagged(t *testing.T) {
	r := Run(nil, []ReportedFill{{OrderID: "ghost", Qty: 5, Price: 100}})
	if r.PhantomFills != 1 || r.TotalFills != 1 || r.ValidFills != 0 {
		t.Fatalf("expected 1 phantom / 0 valid, got %+v", r)
	}
	if r.Violations[0].Type != Phantom {
		t.Fatalf("expected phantom violation, got %v", r.Violations[0].Type)
	}
}

func TestWrongPriceFlagged(t *testing.T) {
	// Reference fills B1 at 100; contestant claims it filled at 101.
	ordered := []*model.Order{
		order("S1", model.NewLimit, model.Sell, 100, 10),
		order("B1", model.NewLimit, model.Buy, 100, 10, fill(10, 101)),
	}
	r := Run(ordered, nil)
	if r.PriceViolations != 1 || r.ValidFills != 0 {
		t.Fatalf("expected 1 price violation / 0 valid, got %+v", r)
	}
}

func TestFillBeyondReferenceQtyFlagged(t *testing.T) {
	// B1 (qty 20) reports 15@100, but only 10 was available to fill at the book.
	ordered := []*model.Order{
		order("S1", model.NewLimit, model.Sell, 100, 10),
		order("B1", model.NewLimit, model.Buy, 100, 20, fill(15, 100)),
	}
	r := Run(ordered, nil)
	if r.PriceViolations != 1 || r.ValidFills != 0 {
		t.Fatalf("expected reported-beyond-reference flagged, got %+v", r)
	}
}

func TestUnderReportIsValid(t *testing.T) {
	// Reference fills 10; contestant reports only 5 — under-fill is not a violation.
	ordered := []*model.Order{
		order("S1", model.NewLimit, model.Sell, 100, 10),
		order("B1", model.NewLimit, model.Buy, 100, 10, fill(5, 100)),
	}
	r := Run(ordered, nil)
	if r.ValidFills != 1 || len(r.Violations) != 0 {
		t.Fatalf("under-report should be valid, got %+v", r)
	}
}

func TestDeterministic(t *testing.T) {
	build := func() []*model.Order {
		return []*model.Order{
			order("S1", model.NewLimit, model.Sell, 100, 20),
			order("B1", model.NewLimit, model.Buy, 100, 10, fill(6, 100), fill(6, 100)),
			order("B2", model.NewLimit, model.Buy, 100, 10, fill(10, 100)),
		}
	}
	r1 := Run(build(), []ReportedFill{{OrderID: "ghost", Qty: 1, Price: 50}})
	r2 := Run(build(), []ReportedFill{{OrderID: "ghost", Qty: 1, Price: 50}})
	if !reflect.DeepEqual(r1, r2) {
		t.Fatalf("validation not deterministic:\n r1=%+v\n r2=%+v", r1, r2)
	}
}
