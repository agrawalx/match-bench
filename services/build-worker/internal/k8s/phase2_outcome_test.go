package k8s

import (
	"errors"
	"testing"

	"github.com/iicpc/schemas/topics"
)

// closedStatuses / closedErrs build the already-closed channels phase2Outcome
// expects (the waiter goroutine closes both after wg.Wait).
func closedStatuses(vals ...string) <-chan string {
	ch := make(chan string, len(vals))
	for _, v := range vals {
		ch <- v
	}
	close(ch)
	return ch
}

func closedErrs(vals ...error) <-chan error {
	ch := make(chan error, len(vals))
	for _, v := range vals {
		ch <- v
	}
	close(ch)
	return ch
}

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

// The finding-36 regression: a partial failure must yield NO success statuses,
// so the caller can never publish a sibling's forward-progress status ahead of
// `failed`.
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
