package trigger

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/iicpc/schemas/topics"
)

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
