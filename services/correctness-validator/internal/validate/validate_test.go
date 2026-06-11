// Package validate defines tests for validate test.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package validate

import (
	"reflect"
	"testing"

	"github.com/iicpc/correctness-validator/internal/model"
)

// fill performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func fill(qty, price uint64) model.Response {
	return model.Response{ExecType: "2", FillQty: qty, FillPrice: price}
}

// order performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func order(id string, kind model.Kind, side model.Side, price int64, qty uint64, resp ...model.Response) *model.Order {
	return &model.Order{OrderID: id, Kind: kind, Side: side, Price: price, Qty: qty, Responses: resp}
}

// TestCleanCrossIsAllValid performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestCleanCrossIsAllValid(t *testing.T) {
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

// TestOverfillFlagged performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestOverfillFlagged(t *testing.T) {
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

// TestPhantomFlagged performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestPhantomFlagged(t *testing.T) {
	r := Run(nil, []ReportedFill{{OrderID: "ghost", Qty: 5, Price: 100}})
	if r.PhantomFills != 1 || r.TotalFills != 1 || r.ValidFills != 0 {
		t.Fatalf("expected 1 phantom / 0 valid, got %+v", r)
	}
	if r.Violations[0].Type != Phantom {
		t.Fatalf("expected phantom violation, got %v", r.Violations[0].Type)
	}
}

// TestWrongPriceFlagged performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestWrongPriceFlagged(t *testing.T) {
	ordered := []*model.Order{
		order("S1", model.NewLimit, model.Sell, 100, 10),
		order("B1", model.NewLimit, model.Buy, 100, 10, fill(10, 101)),
	}
	r := Run(ordered, nil)
	if r.PriceViolations != 1 || r.ValidFills != 0 {
		t.Fatalf("expected 1 price violation / 0 valid, got %+v", r)
	}
}

// TestFillBeyondReferenceQtyFlagged performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestFillBeyondReferenceQtyFlagged(t *testing.T) {
	ordered := []*model.Order{
		order("S1", model.NewLimit, model.Sell, 100, 10),
		order("B1", model.NewLimit, model.Buy, 100, 20, fill(15, 100)),
	}
	r := Run(ordered, nil)
	if r.PriceViolations != 1 || r.ValidFills != 0 {
		t.Fatalf("expected reported-beyond-reference flagged, got %+v", r)
	}
}

// TestUnderReportIsValid performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestUnderReportIsValid(t *testing.T) {
	ordered := []*model.Order{
		order("S1", model.NewLimit, model.Sell, 100, 10),
		order("B1", model.NewLimit, model.Buy, 100, 10, fill(5, 100)),
	}
	r := Run(ordered, nil)
	if r.ValidFills != 1 || len(r.Violations) != 0 {
		t.Fatalf("under-report should be valid, got %+v", r)
	}
}

// TestDeterministic performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
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

// TestWindowsOverlap performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestWindowsOverlap(t *testing.T) {
	if !windowsOverlap(1000, 1500, 1600, 200) { // liquidity [1000,1500] touches 1400..1800 at 1500
		t.Fatal("should overlap: avail exit 1500 within [1400,1800]")
	}
	if windowsOverlap(1000, 1300, 1600, 200) { // avail ended 1300, before 1400
		t.Fatal("should NOT overlap: avail exit 1300 before window 1400")
	}
	if !windowsOverlap(0, ^uint64(0), 5, 1) { // still-resting (exit=max) always overlaps
		t.Fatal("still-resting liquidity should overlap any window")
	}
	if windowsOverlap(5000, ^uint64(0), 100, 50) { // avail entered 5000, after window hi=150
		t.Fatal("should NOT overlap: avail enter 5000 after window hi 150")
	}
}
