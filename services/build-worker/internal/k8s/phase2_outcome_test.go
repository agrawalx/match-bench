// Package k8s defines tests for phase2 outcome test.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package k8s

import (
	"errors"
	"testing"

	"github.com/iicpc/schemas/topics"
)

// closedStatuses performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func closedStatuses(vals ...string) <-chan string {
	ch := make(chan string, len(vals))
	for _, v := range vals {
		ch <- v
	}
	close(ch)
	return ch
}

// closedErrs performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func closedErrs(vals ...error) <-chan error {
	ch := make(chan error, len(vals))
	for _, v := range vals {
		ch <- v
	}
	close(ch)
	return ch
}

// TestPhase2Outcome_BothSucceed_ReturnsAllStatuses performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestPhase2Outcome_BothSucceed_ReturnsAllStatuses(t *testing.T) {
	oks, err := phase2Outcome(
		closedStatuses(topics.StatusScanned, topics.StatusSBOMReady),
		closedErrs(),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(oks) != 2 {
		t.Fatalf("want 2 success statuses, got %v", oks)
	}
}

// TestPhase2Outcome_PartialFailure_PublishesNoSuccessStatus performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestPhase2Outcome_PartialFailure_PublishesNoSuccessStatus(t *testing.T) {
	sbomErr := errors.New("sbom: job timed out")
	oks, err := phase2Outcome(
		closedStatuses(topics.StatusScanned), // scan succeeded
		closedErrs(sbomErr),                  // sbom failed
	)
	if err == nil {
		t.Fatal("expected an error from the failed sbom job")
	}
	if len(oks) != 0 {
		t.Fatalf("partial failure must publish no success status, got %v", oks)
	}
}

// TestPhase2Outcome_BothFail_ReturnsAnError performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestPhase2Outcome_BothFail_ReturnsAnError(t *testing.T) {
	oks, err := phase2Outcome(
		closedStatuses(),
		closedErrs(errors.New("scan: failed"), errors.New("sbom: failed")),
	)
	if err == nil {
		t.Fatal("expected an error when both jobs fail")
	}
	if len(oks) != 0 {
		t.Fatalf("want no statuses on failure, got %v", oks)
	}
}
