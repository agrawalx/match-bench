// Integration test for the correctness-validator I/O layer. Env-gated: it talks
// to a real Kafka cluster and Postgres, so it only runs when both are configured.
//
//	docker compose up -d
//	KAFKA_BROKERS=localhost:9092 \
//	DATABASE_URL='postgres://iicpc:iicpc@localhost:5433/iicpc?sslmode=disable' \
//	  go test ./... -run Integration -v
//
// It produces a synthetic session (orders.sent + orders.acked as MessagePack
// named batches) plus a completed status, then exercises:
//   - source.DrainSession  (read every event for the session off all partitions)
//   - pipeline.Run         (assemble -> order by effective_t3 -> validate)
//   - store.Save           (summary + violation log in one tx)
//   - publisher.Publish     (CorrectnessScoreEvent on scores.correctness)
//   - runTriggerConsumer    (benchmark.status.updated -> dispatch on `completed`)
//
// The session is built so the report is fully predictable: M1 (SELL 100x10) rests,
// T1 (BUY 100x10) crosses -> two valid fills; T1 over-reports a 3rd fill (overfill);
// a phantom fill is reported for an order that was never sent. So: 4 total fills,
// 2 valid, 1 overfill, 1 phantom, score 0.5, 2 violations.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/iicpc/correctness-validator/internal/publisher"
	"github.com/iicpc/correctness-validator/internal/store"
	"github.com/iicpc/schemas/topics"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
	"github.com/vmihailenco/msgpack/v5"
)

func itEnv(key string) string { return strings.TrimSpace(os.Getenv(key)) }

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestIntegration_ValidateSession(t *testing.T) {
	brokersCSV := itEnv("KAFKA_BROKERS")
	dbURL := itEnv("DATABASE_URL")
	if brokersCSV == "" || dbURL == "" {
		t.Skip("set KAFKA_BROKERS and DATABASE_URL to run the validator integration test")
	}
	brokers := parseBrokers(brokersCSV)
	ctx := context.Background()
	sessionID := fmt.Sprintf("itest-%d", time.Now().UnixNano())
	const contestant = "team-itest"

	produceSession(ctx, t, brokers, sessionID, contestant)

	// Snapshot scores.correctness before we publish so we can read only our event.
	since := snapshotOffsets(ctx, t, brokers, topics.TopicScoresCorrectness)

	st, err := store.New(ctx, dbURL)
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}
	defer st.Close()
	pub := publisher.New(brokersCSV)
	defer pub.Close()

	v := &validator{
		log:         discardLogger(),
		brokers:     brokers,
		store:       st,
		pub:         pub,
		settleDelay: 0, // no settle wait in the test
	}
	if err := v.validateSession(ctx, sessionID); err != nil {
		t.Fatalf("validateSession: %v", err)
	}

	// ---- assert the Postgres summary ----
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		t.Fatalf("pgxpool: %v", err)
	}
	defer pool.Close()

	var (
		gotContestant                                      string
		total, valid, vcount, phantom, overfill, priceViol int64
		score                                              float64
	)
	err = pool.QueryRow(ctx, `
SELECT contestant_id, total_fills, valid_fills, violation_count,
       phantom_fills, overfills, price_violations, correctness_score
FROM correctness_summary WHERE session_id=$1`, sessionID).
		Scan(&gotContestant, &total, &valid, &vcount, &phantom, &overfill, &priceViol, &score)
	if err != nil {
		t.Fatalf("query summary: %v", err)
	}
	if gotContestant != contestant {
		t.Errorf("contestant_id = %q, want %q", gotContestant, contestant)
	}
	if total != 4 || valid != 2 || vcount != 2 || phantom != 1 || overfill != 1 || priceViol != 0 {
		t.Errorf("summary counts = total %d valid %d violations %d phantom %d overfill %d price %d; want 4/2/2/1/1/0",
			total, valid, vcount, phantom, overfill, priceViol)
	}
	if score < 0.49 || score > 0.51 {
		t.Errorf("correctness_score = %v, want ~0.5", score)
	}

	// ---- assert the violation log ----
	rows, err := pool.Query(ctx,
		"SELECT violation_type, order_id FROM correctness_violations WHERE session_id=$1 ORDER BY violation_type", sessionID)
	if err != nil {
		t.Fatalf("query violations: %v", err)
	}
	defer rows.Close()
	viol := map[string]string{}
	for rows.Next() {
		var vt, oid string
		if err := rows.Scan(&vt, &oid); err != nil {
			t.Fatalf("scan violation: %v", err)
		}
		viol[vt] = oid
	}
	if len(viol) != 2 {
		t.Errorf("violation rows = %d, want 2 (%v)", len(viol), viol)
	}
	if viol["overfill"] != "T1" {
		t.Errorf("overfill violation order = %q, want T1", viol["overfill"])
	}
	if viol["phantom"] != "PH" {
		t.Errorf("phantom violation order = %q, want PH", viol["phantom"])
	}

	// ---- assert the published CorrectnessScoreEvent ----
	ev, ok := readNewScore(ctx, t, brokers, since, sessionID)
	if !ok {
		t.Fatal("CorrectnessScoreEvent not found on scores.correctness")
	}
	if ev.ContestantID != contestant || ev.TotalFills != 4 || ev.ValidFills != 2 || ev.ViolationCount != 2 {
		t.Errorf("published event = %+v; want contestant %q total 4 valid 2 violations 2", ev, contestant)
	}
	if ev.CorrectnessScore < 0.49 || ev.CorrectnessScore > 0.51 {
		t.Errorf("published score = %v, want ~0.5", ev.CorrectnessScore)
	}

	// ---- idempotency: a second run must short-circuit (HasSummary guard) ----
	done, err := st.HasSummary(ctx, sessionID)
	if err != nil || !done {
		t.Errorf("HasSummary after save = (%v,%v), want (true,nil)", done, err)
	}
}

// TestIntegration_TriggerConsumer drives the real runStatusConsumer end to end:
// a `completed` benchmark.status.updated event must trigger drain -> validate ->
// durable persist (M21: the offset commits only after the summary lands, so the
// session is never silently dropped). We assert the summary row appears.
//
// Env-gated: needs KAFKA_BROKERS + DATABASE_URL.
func TestIntegration_TriggerConsumer(t *testing.T) {
	brokersCSV := itEnv("KAFKA_BROKERS")
	dbURL := itEnv("DATABASE_URL")
	if brokersCSV == "" || dbURL == "" {
		t.Skip("set KAFKA_BROKERS + DATABASE_URL to run the trigger-consumer integration test")
	}
	brokers := parseBrokers(brokersCSV)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sessionID := fmt.Sprintf("itest-trigger-%d", time.Now().UnixNano())
	const contestant = "team-trigger"

	produceSession(ctx, t, brokers, sessionID, contestant)
	produceStatus(ctx, t, brokers, sessionID, topics.RunStatusRunning) // ignored (non-terminal)
	produceStatus(ctx, t, brokers, sessionID, topics.RunStatusCompleted)

	st, err := store.New(ctx, dbURL)
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	defer st.Close()
	pub := publisher.New(brokersCSV)
	defer pub.Close()
	v := &validator{log: discardLogger(), brokers: brokers, store: st, pub: pub, settleDelay: 0}

	group := fmt.Sprintf("itest-validator-%d", time.Now().UnixNano())
	go v.runStatusConsumer(ctx, brokers, group)

	deadline := time.After(90 * time.Second)
	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			if done, _ := st.HasSummary(ctx, sessionID); done {
				return // consumer triggered validation and durably persisted the score
			}
		case <-deadline:
			t.Fatal("timed out waiting for the completed session to be validated")
		}
	}
}

// ---- producers -------------------------------------------------------------

func produceSession(ctx context.Context, t *testing.T, brokers []string, sessionID, contestant string) {
	t.Helper()
	sent := topics.OrderSentBatch{
		SessionID: sessionID,
		WorkerID:  "w0",
		Events: []topics.OrderSentEvent{
			{SessionID: sessionID, OrderID: "M1", Price: 100, Qty: 10, Side: "SELL", PayloadType: "NEW", OrdType: "LIMIT"},
			{SessionID: sessionID, OrderID: "T1", Price: 100, Qty: 10, Side: "BUY", PayloadType: "NEW", OrdType: "LIMIT"},
		},
	}
	acked := topics.OrderAckedBatch{
		SessionID:    sessionID,
		ContestantID: contestant,
		Events: []topics.OrderAckedEvent{
			// M1 rests first (smallest t3), then reports its fill.
			{SessionID: sessionID, ContestantID: contestant, OrderID: "M1", SrcIP: 1, SrcPort: 1000, TCPSeq: 1, T3XDPIngressNS: 1000, T7XDPEgressNS: 1500, ExecType: "0"},
			{SessionID: sessionID, ContestantID: contestant, OrderID: "M1", SrcIP: 1, SrcPort: 1000, TCPSeq: 1, T3XDPIngressNS: 1000, T7XDPEgressNS: 1600, ExecType: "2", FillQty: 10, FillPrice: 100 * topics.TelemetryPriceScale},
			// T1 crosses; reports two legit fills then an over-reported third.
			{SessionID: sessionID, ContestantID: contestant, OrderID: "T1", SrcIP: 2, SrcPort: 2000, TCPSeq: 1, T3XDPIngressNS: 2000, T7XDPEgressNS: 2500, ExecType: "0"},
			{SessionID: sessionID, ContestantID: contestant, OrderID: "T1", SrcIP: 2, SrcPort: 2000, TCPSeq: 1, T3XDPIngressNS: 2000, T7XDPEgressNS: 2600, ExecType: "2", FillQty: 10, FillPrice: 100 * topics.TelemetryPriceScale},
			{SessionID: sessionID, ContestantID: contestant, OrderID: "T1", SrcIP: 2, SrcPort: 2000, TCPSeq: 1, T3XDPIngressNS: 2000, T7XDPEgressNS: 2700, ExecType: "2", FillQty: 5, FillPrice: 100 * topics.TelemetryPriceScale},
			// PH: a fill reported for an order that was never sent.
			{SessionID: sessionID, ContestantID: contestant, OrderID: "PH", SrcIP: 3, SrcPort: 3000, TCPSeq: 1, T3XDPIngressNS: 3000, T7XDPEgressNS: 3100, ExecType: "2", FillQty: 3, FillPrice: 100 * topics.TelemetryPriceScale},
		},
	}
	writeMsgpack(ctx, t, brokers, topics.TopicOrdersSent, sessionID, sent)
	writeMsgpack(ctx, t, brokers, topics.TopicOrdersAcked, sessionID, acked)
}

func writeMsgpack(ctx context.Context, t *testing.T, brokers []string, topic, key string, v any) {
	t.Helper()
	payload, err := msgpack.Marshal(v) // named maps, matching the Rust rmp_serde::to_vec_named producers
	if err != nil {
		t.Fatalf("msgpack marshal %s: %v", topic, err)
	}
	w := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireAll,
	}
	defer w.Close()
	if err := w.WriteMessages(ctx, kafka.Message{Key: []byte(key), Value: payload}); err != nil {
		t.Fatalf("write %s: %v", topic, err)
	}
}

func produceStatus(ctx context.Context, t *testing.T, brokers []string, sessionID, status string) {
	t.Helper()
	payload, err := json.Marshal(topics.BenchmarkStatusUpdated{SessionID: sessionID, Status: status})
	if err != nil {
		t.Fatalf("marshal status: %v", err)
	}
	w := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topics.TopicBenchmarkStatusUpdated,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireAll,
	}
	defer w.Close()
	if err := w.WriteMessages(ctx, kafka.Message{Key: []byte(sessionID), Value: payload}); err != nil {
		t.Fatalf("write status: %v", err)
	}
}

// ---- scores.correctness read-back ------------------------------------------

func snapshotOffsets(ctx context.Context, t *testing.T, brokers []string, topic string) map[int]int64 {
	t.Helper()
	conn, err := kafka.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	parts, err := conn.ReadPartitions(topic)
	conn.Close()
	if err != nil {
		t.Fatalf("read partitions %s: %v", topic, err)
	}
	out := make(map[int]int64, len(parts))
	for _, p := range parts {
		c, err := kafka.DialLeader(ctx, "tcp", brokers[0], topic, p.ID)
		if err != nil {
			t.Fatalf("dial leader %s/%d: %v", topic, p.ID, err)
		}
		last, err := c.ReadLastOffset()
		c.Close()
		if err != nil {
			t.Fatalf("read last offset %s/%d: %v", topic, p.ID, err)
		}
		out[p.ID] = last
	}
	return out
}

func readNewScore(ctx context.Context, t *testing.T, brokers []string, since map[int]int64, sessionID string) (*topics.CorrectnessScoreEvent, bool) {
	t.Helper()
	for pid, start := range since {
		r := kafka.NewReader(kafka.ReaderConfig{
			Brokers:   brokers,
			Topic:     topics.TopicScoresCorrectness,
			Partition: pid,
			MinBytes:  1,
			MaxBytes:  1 << 20,
		})
		if err := r.SetOffset(start); err != nil {
			r.Close()
			t.Fatalf("set offset %d: %v", pid, err)
		}
		for {
			rctx, cancel := context.WithTimeout(ctx, 3*time.Second)
			m, err := r.ReadMessage(rctx)
			cancel()
			if err != nil {
				break // caught up to watermark (deadline) or partition end
			}
			var ev topics.CorrectnessScoreEvent
			if json.Unmarshal(m.Value, &ev) == nil && ev.SessionID == sessionID {
				r.Close()
				return &ev, true
			}
		}
		r.Close()
	}
	return nil, false
}
