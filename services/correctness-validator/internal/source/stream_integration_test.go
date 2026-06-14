package source

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/iicpc/correctness-validator/internal/pipeline"
	"github.com/iicpc/correctness-validator/internal/validate"
	"github.com/iicpc/schemas/topics"
	"github.com/segmentio/kafka-go"
	"github.com/vmihailenco/msgpack/v5"
)

func mkID(sid string, bot, seq int) string { return fmt.Sprintf("%s_%d_%d_O", sid, bot, seq) }

func ack(sid, id string, port uint16, seq uint32, t3 uint64, exec string, fq, fp uint64) topics.OrderAckedEvent {
	return topics.OrderAckedEvent{
		SessionID: sid, ContestantID: "team-x", OrderID: id,
		SrcIP: 0x0a000001, SrcPort: port, TCPSeq: seq,
		T3XDPIngressNS: t3, T7XDPEgressNS: t3 + 500, PodServiceTimeNS: 500,
		ExecType: exec, FillQty: fq, FillPrice: fp,
	}
}

func writeAcked(ctx context.Context, t *testing.T, brokers []string, sid string, events []topics.OrderAckedEvent) {
	t.Helper()
	payload, err := msgpack.Marshal(topics.OrderAckedBatch{SessionID: sid, ContestantID: "team-x", Events: events})
	if err != nil {
		t.Fatalf("msgpack marshal acked: %v", err)
	}
	w := &kafka.Writer{Addr: kafka.TCP(brokers...), Topic: topics.TopicOrdersAcked, Balancer: &kafka.LeastBytes{}, RequiredAcks: kafka.RequireAll}
	defer w.Close()
	if err := w.WriteMessages(ctx, kafka.Message{Key: []byte(sid), Value: payload}); err != nil {
		t.Fatalf("write orders.acked: %v", err)
	}
}

// TestIntegration_StreamEquivalentToBatch produces a real session to Kafka and asserts
// the streaming source + StreamValidator yields the SAME score as the batch path
// (DrainSession + pipeline.Run). Needs a live broker: set KAFKA_BROKERS.
func TestIntegration_StreamEquivalentToBatch(t *testing.T) {
	brokersCSV := strings.TrimSpace(os.Getenv("KAFKA_BROKERS"))
	if brokersCSV == "" {
		t.Skip("set KAFKA_BROKERS to run the stream-vs-batch equivalence integration test")
	}
	brokers := strings.Split(brokersCSV, ",")
	ctx := context.Background()
	sc := uint64(topics.TelemetryPriceScale)
	sid := newUUIDv7(time.Now())

	// realistic ns timestamps spread ~1s apart so the mid-stream windowed emit path
	// (not just the final flush) is exercised; sent precedes its ack by ~5ms.
	base := uint64(1_700_000_000_000_000_000)
	t3 := func(i int) uint64 { return base + uint64(i)*1_000_000_000 }
	st := func(i int) uint64 { return t3(i) - 5_000_000 }
	sent := func(bot, seq, i int, price, qty uint64, side string) topics.OrderSentEvent {
		return topics.OrderSentEvent{SessionID: sid, OrderID: mkID(sid, bot, seq), SendTSNS: st(i),
			Price: price, Qty: qty, Side: side, PayloadType: "NEW", OrdType: "LIMIT"}
	}
	sents := []topics.OrderSentEvent{
		sent(7, 1, 0, 100, 10, "SELL"),
		sent(7, 2, 1, 100, 10, "SELL"),
		sent(8, 3, 2, 100, 10, "BUY"),
		sent(8, 4, 3, 101, 5, "SELL"),
		sent(9, 5, 4, 100, 10, "BUY"),
	}
	writeSent(ctx, t, brokers, sid, sents)

	acks := []topics.OrderAckedEvent{
		ack(sid, mkID(sid, 7, 1), 7, 1, t3(0), "0", 0, 0),       // S1 New (filled by reference)
		ack(sid, mkID(sid, 7, 2), 7, 2, t3(1), "0", 0, 0),       // S2 New (rests)
		ack(sid, mkID(sid, 8, 3), 8, 3, t3(2), "2", 10, 100*sc), // B1 valid fill 10@100
		ack(sid, mkID(sid, 8, 4), 8, 4, t3(3), "0", 0, 0),       // S4 New (rests @101)
		ack(sid, mkID(sid, 9, 5), 9, 5, t3(4), "2", 10, 100*sc), // B2 fill, ref book empty at 100 -> violation
		ack(sid, "ghost", 9, 6, t3(5), "2", 5, 50*sc),           // phantom (never sent)
	}
	writeAcked(ctx, t, brokers, sid, acks)

	// batch path
	bSents, bAckeds, err := DrainSession(ctx, brokers, sid)
	if err != nil {
		t.Fatalf("DrainSession: %v", err)
	}
	bReport, _, bContestant := pipeline.Run(bSents, bAckeds)

	// streaming path
	sv := validate.NewStreamValidator()
	sCounts, sContestant, err := StreamSession(ctx, brokers, sid, 0,
		sv.Apply,
		func(id string, qty uint64, price int64) { sv.AddPhantom(validate.ReportedFill{OrderID: id, Qty: qty, Price: price}) },
	)
	if err != nil {
		t.Fatalf("StreamSession: %v", err)
	}
	sReport := sv.Finish()

	if bContestant != sContestant {
		t.Errorf("contestant mismatch: batch=%q stream=%q", bContestant, sContestant)
	}
	if sCounts.MatchedOrders == 0 {
		t.Fatalf("stream matched 0 orders — join/produce likely broken: %+v", sCounts)
	}
	// score-equivalence (Time/Price may be reclassified — compared as a sum)
	if bReport.TotalFills != sReport.TotalFills ||
		bReport.ValidFills != sReport.ValidFills ||
		bReport.PhantomFills != sReport.PhantomFills ||
		bReport.Overfills != sReport.Overfills ||
		bReport.SelfTrades != sReport.SelfTrades ||
		(bReport.TimeViolations+bReport.PriceViolations) != (sReport.TimeViolations+sReport.PriceViolations) ||
		bReport.ViolationCount() != sReport.ViolationCount() ||
		bReport.CorrectnessScore() != sReport.CorrectnessScore() {
		t.Fatalf("stream != batch score:\n  batch =%+v score=%.6f\n  stream=%+v score=%.6f",
			bReport, bReport.CorrectnessScore(), sReport, sReport.CorrectnessScore())
	}
	t.Logf("OK: matched=%d score=%.4f total=%d valid=%d violations=%d",
		sCounts.MatchedOrders, sReport.CorrectnessScore(), sReport.TotalFills, sReport.ValidFills, sReport.ViolationCount())
}
