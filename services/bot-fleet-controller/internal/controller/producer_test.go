package controller

import (
	"log/slog"
	"testing"

	kafka "github.com/segmentio/kafka-go"
)

// TestKeyedWritersUseKeySensitiveBalancer locks in the finding-19 fix: all three
// controller writers must hash the message Key so a session_id deterministically
// maps to one partition (per-session ordering). LeastBytes ignores the Key and
// scatters a session across partitions once a topic has >1 partition — this test
// guards against a silent revert to it.
func TestKeyedWritersUseKeySensitiveBalancer(t *testing.T) {
	p := NewProducer("localhost:9092", slog.Default())
	t.Cleanup(func() { _ = p.Close() })

	for name, w := range map[string]*kafka.Writer{
		"workload": p.workloadWriter,
		"barrier":  p.barrierWriter,
		"status":   p.statusWriter,
	} {
		if _, ok := w.Balancer.(*kafka.Hash); !ok {
			t.Errorf("%s writer balancer = %T, want *kafka.Hash (key-sensitive)", name, w.Balancer)
		}
	}
}

// TestHashBalancerRoutesSameKeyToSamePartition documents the property the fix
// relies on: identical keys land on one partition, and the Key actually affects
// routing (LeastBytes would not). Uses kafka.Hash directly so it needs no broker.
func TestHashBalancerRoutesSameKeyToSamePartition(t *testing.T) {
	var b kafka.Hash
	parts := []int{0, 1, 2, 3, 4, 5}

	a := b.Balance(kafka.Message{Key: []byte("session-A")}, parts...)
	aAgain := b.Balance(kafka.Message{Key: []byte("session-A")}, parts...)
	if a != aAgain {
		t.Fatalf("same key routed to different partitions: %d vs %d", a, aAgain)
	}

	// The key must actually drive routing — at least one distinct key should
	// land on a different partition (otherwise the balancer ignores the key).
	differs := false
	for _, k := range []string{"session-B", "session-C", "session-D", "session-E"} {
		if b.Balance(kafka.Message{Key: []byte(k)}, parts...) != a {
			differs = true
			break
		}
	}
	if !differs {
		t.Fatal("Hash balancer routed every key to the same partition — key not honored")
	}
}
