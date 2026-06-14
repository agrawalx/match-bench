package source

import (
	"testing"

	"github.com/iicpc/schemas/topics"
	"github.com/vmihailenco/msgpack/v5"
)

// The Rust bot-fleet now adds barrier_epoch_ns to OrderSentEvent (for the
// distributed ingester's deterministic wave bucketing). The correctness-validator
// decodes the SAME OrderSentBatch messages, so it must (a) tolerate the new field
// and (b) tolerate any future unknown field a newer producer might add — otherwise
// a rolling upgrade breaks the validator's drain. This test marshals a batch the
// way the Rust `to_vec_named` producer does (named maps) including barrier_epoch_ns
// plus an unknown key, and confirms the Go decoder accepts it.
func TestOrderSentBatchDecodesNewAndUnknownFields(t *testing.T) {
	event := map[string]interface{}{
		"session_id":        "sess-1",
		"submission_id":     "sub-1",
		"worker_id":         "w-1",
		"task_id":           uint32(7),
		"order_id":          "sess-1_7_3_O",
		"target_send_ts_ns": uint64(100),
		"send_ts_ns":        uint64(110),
		"recv_done_ts_ns":   uint64(0),
		"timed_out":         false,
		"price":             uint64(1),
		"qty":               uint64(1),
		"side":              "BUY",
		"payload_type":      "NEW",
		"ord_type":          "LIMIT",
		"orig_order_id":     "",
		"barrier_epoch_ns":  uint64(1_770_000_000_000_000_000),
		// A field a future producer might add — the decoder must ignore it, not error.
		"some_future_field": "ignored",
	}
	batch := map[string]interface{}{
		"session_id": "sess-1",
		"worker_id":  "w-1",
		"events":     []interface{}{event},
	}

	payload, err := msgpack.Marshal(batch)
	if err != nil {
		t.Fatalf("marshal batch: %v", err)
	}

	var decoded topics.OrderSentBatch
	if err := msgpack.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("decode must tolerate new + unknown fields, got: %v", err)
	}
	if len(decoded.Events) != 1 {
		t.Fatalf("want 1 event, got %d", len(decoded.Events))
	}
	got := decoded.Events[0]
	if got.BarrierEpochNs != 1_770_000_000_000_000_000 {
		t.Fatalf("barrier_epoch_ns not decoded: got %d", got.BarrierEpochNs)
	}
	if got.OrderID != "sess-1_7_3_O" {
		t.Fatalf("order_id corrupted: %q", got.OrderID)
	}
}

// A message from a PRE-field producer (no barrier_epoch_ns) must still decode,
// defaulting the field to 0 — the reverse rolling-upgrade direction.
func TestOrderSentBatchDecodesPreFieldMessage(t *testing.T) {
	event := map[string]interface{}{
		"session_id":        "sess-1",
		"submission_id":     "sub-1",
		"worker_id":         "w-1",
		"task_id":           uint32(7),
		"order_id":          "sess-1_7_3_O",
		"target_send_ts_ns": uint64(100),
		"send_ts_ns":        uint64(110),
		"recv_done_ts_ns":   uint64(0),
		"timed_out":         false,
		"price":             uint64(1),
		"qty":               uint64(1),
		"side":              "BUY",
		"payload_type":      "NEW",
		"ord_type":          "LIMIT",
		"orig_order_id":     "",
	}
	payload, err := msgpack.Marshal(map[string]interface{}{
		"session_id": "sess-1", "worker_id": "w-1", "events": []interface{}{event},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var decoded topics.OrderSentBatch
	if err := msgpack.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("decode pre-field message: %v", err)
	}
	if decoded.Events[0].BarrierEpochNs != 0 {
		t.Fatalf("missing field must default to 0, got %d", decoded.Events[0].BarrierEpochNs)
	}
}
