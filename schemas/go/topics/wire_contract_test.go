// Package topics tests the CROSS-LANGUAGE wire contract for orders.sent.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package topics

import (
	"encoding/base64"
	"testing"

	"github.com/vmihailenco/msgpack/v5"
)

// realProducerBatch is a VERBATIM orders.sent record captured off the broker, written
// by the Rust bot-fleet producer (OrderSentBatchV2Ref). It is the fixture rather than a
// Go-encoded round trip on purpose: a Go-to-Go round trip passes even when Go and Rust
// disagree, which is exactly how the bug this test exists for went unnoticed.
//
// The bug: the producer moved orders.sent to a positional envelope with session_id /
// submission_id / worker_id hoisted, and telemetry-ingester was updated — but the
// correctness-validator kept decoding the old named-map OrderSentBatch. Positional
// bytes read as a named map produce a garbage SessionID, so the validator's session
// filter dropped EVERY batch: it reported sent=0 against a topic holding ~600k records,
// which zeroed matched and silently invalidated every correctness score.
const realProducerBatch = "lNkkMDE5ZmFmNWQtY2YyMy03MTAzLTg3YjUtMWJhZTQyZmViZTgwrmIxYS0xNzg1MzUzNTg42SFib3QtZmxlZXQtd29ya2VyLTViZDhmYjQ1NmQtem4yazeWnVrZKzAxOWZhZjVkLWNmMjMtNzEwMy04N2I1LTFiYWU0MmZlYmU4MF85MF80X0/PGMbZpE+ZJcPPGMbZpFKZW1jPGMbZpFKvcrLCzSbtAaNCVVmjTkVXpUxJTUlUoM8YxtmkK9Xfw50e2SswMTlmYWY1ZC1jZjIzLTcxMDMtODdiNS0xYmFlNDJmZWJlODBfMzBfMl9PzxjG2aQ3waHDzxjG2aRSoBL+zxjG2aRS1JZaws0nDwGjQlVZo05FV6VMSU1JVKDPGMbZpCvV38OdHtkrMDE5ZmFmNWQtY2YyMy03MTAzLTg3YjUtMWJhZTQyZmViZTgwXzMwXzRfTc8YxtmkT5klw88YxtmkUqAS/s8YxtmkUtSca8IAAaNCVVmjTkVXpk1BUktFVKDPGMbZpCvV38OdHNkrMDE5ZmFmNWQtY2YyMy03MTAzLTg3YjUtMWJhZTQyZmViZTgwXzI4XzFfT88YxtmkK9Xfw88YxtmkUqXbt88YxtmkUu52msLNJx8IpFNFTEyjTkVXpUxJTUlUoM8YxtmkK9Xfw53MjdksMDE5ZmFmNWQtY2YyMy03MTAzLTg3YjUtMWJhZTQyZmViZTgwXzE0MV8xX03PGMbZpCvV38PPGMbZpFKnBQnPGMbZpFLv7m7CAAWkU0VMTKNORVemTUFSS0VUoM8YxtmkK9Xfw50/2SswMTlmYWY1ZC1jZjIzLTcxMDMtODdiNS0xYmFlNDJmZWJlODBfNjNfMl9PzxjG2aQ3waHDzxjG2aRSqNnPzxjG2aRS8EUJws0m3wejQlVZo05FV6VMSU1JVKDPGMbZpCvV38M="

// TestOrderSentBatchV2WireContract decodes real producer bytes and pins every field
// position. If Rust's field order changes, this fails here instead of silently zeroing
// a benchmark's correctness score.
func TestOrderSentBatchV2WireContract(t *testing.T) {
	raw, err := base64.StdEncoding.DecodeString(realProducerBatch)
	if err != nil {
		t.Fatalf("decode fixture: %v", err)
	}
	// 0x94 = msgpack array of 4. A map-encoded envelope would start 0x83/0x84.
	if raw[0] != 0x94 {
		t.Fatalf("envelope first byte = %#x, want 0x94 (array of 4): the producer writes a POSITIONAL envelope", raw[0])
	}

	var b OrderSentBatchV2
	if err := msgpack.Unmarshal(raw, &b); err != nil {
		t.Fatalf("decode OrderSentBatchV2 from real producer bytes: %v", err)
	}

	const wantSession = "019faf5d-cf23-7103-87b5-1bae42febe80"
	if b.SessionID != wantSession {
		t.Errorf("SessionID = %q, want %q", b.SessionID, wantSession)
	}
	if b.SubmissionID != "b1a-1785353588" {
		t.Errorf("SubmissionID = %q — envelope[1] must be submission_id", b.SubmissionID)
	}
	if b.WorkerID != "bot-fleet-worker-5bd8fb456d-zn2k7" {
		t.Errorf("WorkerID = %q — envelope[2] must be worker_id", b.WorkerID)
	}
	if len(b.Events) != 6 {
		t.Fatalf("Events = %d, want 6", len(b.Events))
	}

	// First event, field by field, against the values in the captured bytes.
	e := b.Events[0]
	if e.TaskID != 90 {
		t.Errorf("TaskID = %d, want 90", e.TaskID)
	}
	if want := wantSession + "_90_4_O"; e.OrderID != want {
		t.Errorf("OrderID = %q, want %q", e.OrderID, want)
	}
	if e.TargetSendTSNS != 1785353602032281027 {
		t.Errorf("TargetSendTSNS = %d, want 1785353602032281027", e.TargetSendTSNS)
	}
	if e.SendTSNS != 1785353602082626392 {
		t.Errorf("SendTSNS = %d, want 1785353602082626392", e.SendTSNS)
	}
	if e.RecvDoneTSNS != 1785353602084074162 {
		t.Errorf("RecvDoneTSNS = %d, want 1785353602084074162", e.RecvDoneTSNS)
	}
	if e.TimedOut {
		t.Error("TimedOut = true, want false")
	}
	if e.Price != 9965 {
		t.Errorf("Price = %d, want 9965", e.Price)
	}
	if e.Qty != 1 {
		t.Errorf("Qty = %d, want 1", e.Qty)
	}
	// Enums cross the wire as UPPERCASE strings (Rust #[serde(rename_all = "UPPERCASE")]).
	if e.Side != "BUY" {
		t.Errorf("Side = %q, want \"BUY\"", e.Side)
	}
	if e.PayloadType != "NEW" {
		t.Errorf("PayloadType = %q, want \"NEW\"", e.PayloadType)
	}
	if e.OrdType != "LIMIT" {
		t.Errorf("OrdType = %q, want \"LIMIT\"", e.OrdType)
	}
	if e.OrigOrderID != "" {
		t.Errorf("OrigOrderID = %q, want empty for a NEW order", e.OrigOrderID)
	}
	if e.BarrierEpochNs != 1785353601432281027 {
		t.Errorf("BarrierEpochNs = %d, want 1785353601432281027", e.BarrierEpochNs)
	}

	// A MARKET/SELL event elsewhere in the batch proves the enum decoding is not just
	// matching the first event's values by luck.
	var sawMarket bool
	for _, ev := range b.Events {
		if ev.OrdType == "MARKET" {
			sawMarket = true
			if ev.Side != "SELL" && ev.Side != "BUY" {
				t.Errorf("MARKET event has Side = %q", ev.Side)
			}
		}
	}
	if !sawMarket {
		t.Error("fixture should contain a MARKET order; enum decoding is under-tested without one")
	}
}

// TestOrderSentBatchV2IntoEvents checks the hoisted envelope fields are stamped onto
// every reconstituted event, mirroring Rust's OrderSentBatchV2::into_events.
func TestOrderSentBatchV2IntoEvents(t *testing.T) {
	raw, err := base64.StdEncoding.DecodeString(realProducerBatch)
	if err != nil {
		t.Fatalf("decode fixture: %v", err)
	}
	var b OrderSentBatchV2
	if err := msgpack.Unmarshal(raw, &b); err != nil {
		t.Fatalf("decode: %v", err)
	}

	events := b.IntoEvents()
	if len(events) != len(b.Events) {
		t.Fatalf("IntoEvents returned %d events, want %d", len(events), len(b.Events))
	}
	for i, e := range events {
		if e.SessionID != b.SessionID {
			t.Errorf("event %d SessionID = %q, want the envelope's %q", i, e.SessionID, b.SessionID)
		}
		if e.SubmissionID != b.SubmissionID {
			t.Errorf("event %d SubmissionID = %q, want %q", i, e.SubmissionID, b.SubmissionID)
		}
		if e.WorkerID != b.WorkerID {
			t.Errorf("event %d WorkerID = %q, want %q", i, e.WorkerID, b.WorkerID)
		}
		if e.OrderID != b.Events[i].OrderID {
			t.Errorf("event %d OrderID = %q, want %q", i, e.OrderID, b.Events[i].OrderID)
		}
	}
}

// TestOrderSentBatchV1DecodeFailsOnV2Bytes documents the actual failure mode, so the
// regression is legible rather than folklore: the OLD named-map struct does not error
// on positional bytes — it silently produces a wrong SessionID, which is why a
// session-id filter dropped everything while nothing logged a decode error.
func TestOrderSentBatchV1DecodeFailsOnV2Bytes(t *testing.T) {
	raw, err := base64.StdEncoding.DecodeString(realProducerBatch)
	if err != nil {
		t.Fatalf("decode fixture: %v", err)
	}
	var old OrderSentBatch
	err = msgpack.Unmarshal(raw, &old)
	if err == nil && old.SessionID == "019faf5d-cf23-7103-87b5-1bae42febe80" {
		t.Fatal("the named-map struct decoded V2 bytes correctly; " +
			"if the producer reverted to a map envelope, OrderSentBatchV2 and its consumers must be revisited")
	}
}
