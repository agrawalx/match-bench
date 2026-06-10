package controller

import (
	"fmt"
	"log/slog"
	"testing"

	"github.com/iicpc/schemas/topics"
	kafka "github.com/segmentio/kafka-go"
)

// TestKeyedWritersUseKeySensitiveBalancer locks in the finding-19 fix for the
// control writers: barrier and status writers must hash the message Key so a
// session_id deterministically maps to one partition (per-session ordering).
// LeastBytes ignores the Key and scatters a session across partitions once a
// topic has >1 partition — this test guards against a silent revert to it.
//
// The workload writer is the exception: hash collisions on
// session_id:worker_index keys can put two WorkloadSpecs on one partition
// (one worker runs both serially, the second misses the barrier, another
// worker idles), so it pins each spec to partition worker_index%N via
// workerIndexBalancer instead.
func TestKeyedWritersUseKeySensitiveBalancer(t *testing.T) {
	p := NewProducer("localhost:9092", slog.Default())
	t.Cleanup(func() { _ = p.Close() })

	for name, w := range map[string]*kafka.Writer{
		"barrier": p.barrierWriter,
		"status":  p.statusWriter,
	} {
		if _, ok := w.Balancer.(*kafka.Hash); !ok {
			t.Errorf("%s writer balancer = %T, want *kafka.Hash (key-sensitive)", name, w.Balancer)
		}
	}
	if _, ok := p.workloadWriter.Balancer.(*workerIndexBalancer); !ok {
		t.Errorf("workload writer balancer = %T, want *workerIndexBalancer (explicit per-worker partition)", p.workloadWriter.Balancer)
	}
}

// TestWorkerPartitionMapsIndexModuloPartitions pins the pure partition
// assignment: worker_index i publishes to partition i%numPartitions, so with
// worker_count <= partitions every worker pod owns exactly one spec. The old
// kafka.Hash keying could collide two specs onto one partition — one worker
// runs them serially (the second misses the barrier) while another idles,
// degrading most multi-worker (spike/ramp) runs.
func TestWorkerPartitionMapsIndexModuloPartitions(t *testing.T) {
	tests := []struct {
		name          string
		workerIndex   uint32
		numPartitions int
		want          int
	}{
		{"index 0 pins partition 0", 0, 24, 0},
		{"index 7 pins partition 7", 7, 24, 7},
		{"index 23 pins last partition", 23, 24, 23},
		{"index wraps modulo partition count", 24, 24, 0},
		{"index 30 wraps to partition 6", 30, 24, 6},
		{"zero partitions clamps to 0", 5, 0, 0},
		{"negative partitions clamps to 0", 5, -1, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := workerPartition(tt.workerIndex, tt.numPartitions); got != tt.want {
				t.Errorf("workerPartition(%d, %d) = %d, want %d", tt.workerIndex, tt.numPartitions, got, tt.want)
			}
		})
	}
}

// TestWorkerIndexBalancerRoutesByHint exercises the balancer the workload
// writer installs: every message carries its WorkerIndex as a WriterData hint
// and must land on exactly partition worker_index%N — collision-free for all
// worker_index < N, unlike key hashing. A message without the hint (defensive
// only; PublishWorkloadSpec always sets it) falls back to keyed hashing and
// must stay deterministic and in range.
func TestWorkerIndexBalancerRoutesByHint(t *testing.T) {
	b := &workerIndexBalancer{}
	parts := make([]int, 24)
	for i := range parts {
		parts[i] = i
	}

	for i := uint32(0); i < 24; i++ {
		msg := kafka.Message{
			Key:        []byte(fmt.Sprintf("sess-1:%d", i)),
			WriterData: i,
		}
		if got := b.Balance(msg, parts...); got != int(i) {
			t.Errorf("Balance(worker_index=%d) = partition %d, want %d", i, got, i)
		}
	}

	// No hint → keyed-hash fallback: deterministic and within the partition set.
	noHint := kafka.Message{Key: []byte("sess-1:0")}
	first := b.Balance(noHint, parts...)
	if again := b.Balance(noHint, parts...); again != first {
		t.Errorf("fallback not deterministic: %d vs %d", first, again)
	}
	if first < 0 || first >= len(parts) {
		t.Errorf("fallback partition %d out of range [0,%d)", first, len(parts))
	}
}

// TestBuildWorkloadMessagesCarriesWorkerIndexHint pins the message-assembly
// contract: key stays session_id:worker_index (consumer-side identification,
// integration tests) and WriterData carries the WorkerIndex hint that
// workerIndexBalancer routes on.
func TestBuildWorkloadMessagesCarriesWorkerIndexHint(t *testing.T) {
	specs := []topics.WorkloadSpec{
		{SessionID: "sess-1", WorkerIndex: 0, WorkerCount: 3},
		{SessionID: "sess-1", WorkerIndex: 1, WorkerCount: 3},
		{SessionID: "sess-1", WorkerIndex: 2, WorkerCount: 3},
	}
	msgs, err := buildWorkloadMessages(specs)
	if err != nil {
		t.Fatalf("buildWorkloadMessages: %v", err)
	}
	if len(msgs) != len(specs) {
		t.Fatalf("got %d messages, want %d", len(msgs), len(specs))
	}
	for i, msg := range msgs {
		wantKey := fmt.Sprintf("sess-1:%d", i)
		if string(msg.Key) != wantKey {
			t.Errorf("message %d key = %q, want %q", i, msg.Key, wantKey)
		}
		hint, ok := msg.WriterData.(uint32)
		if !ok {
			t.Fatalf("message %d WriterData = %T, want uint32 worker index", i, msg.WriterData)
		}
		if hint != uint32(i) {
			t.Errorf("message %d WriterData = %d, want %d", i, hint, i)
		}
	}
}

// TestValidateWorkerCapacity pins the hard worker_count<=partitions error:
// with more workers than partitions the modulo wraps and two specs share a
// partition — exactly the serial-execution collision the explicit assignment
// exists to prevent — so the publish must fail loudly instead.
func TestValidateWorkerCapacity(t *testing.T) {
	tests := []struct {
		name        string
		workerCount int
		partitions  int
		wantErr     bool
	}{
		{"one worker one partition", 1, 1, false},
		{"workers below partition count", 5, 24, false},
		{"workers equal partition count", 24, 24, false},
		{"workers exceed partition count", 25, 24, true},
		{"no partitions reported", 1, 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateWorkerCapacity(tt.workerCount, tt.partitions)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateWorkerCapacity(%d, %d) error = %v, wantErr %v", tt.workerCount, tt.partitions, err, tt.wantErr)
			}
		})
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
