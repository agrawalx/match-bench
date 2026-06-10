// Package trigger defines tests for consumer test.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package trigger

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/iicpc/schemas/topics"
)

// TestDecodeStatus performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestDecodeStatus(t *testing.T) {
	want := topics.BenchmarkStatusUpdated{
		SessionID:    "s",
		RunGroupID:   "rg",
		Status:       topics.RunStatusCompleted,
		UpdatedAt:    time.Unix(1, 2).UTC(),
		SubmissionID: "sub",
	}
	b, _ := json.Marshal(want)
	got, err := DecodeStatus(b)
	if err != nil {
		t.Fatal(err)
	}
	if got.SessionID != want.SessionID || got.RunGroupID != want.RunGroupID || got.Status != want.Status {
		t.Fatalf("got %#v want %#v", got, want)
	}
}
