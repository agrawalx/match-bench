// Package controller defines tests for producer test.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package controller

import (
	"fmt"
	"log/slog"
	"testing"

	"github.com/iicpc/schemas/topics"
	kafka "github.com/segmentio/kafka-go"
)

// TestKeyedWritersUseKeySensitiveBalancer performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
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

// TestWorkerPartitionMapsIndexModuloPartitions performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
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

// TestWorkerIndexBalancerRoutesByHint performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
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

	noHint := kafka.Message{Key: []byte("sess-1:0")}
	first := b.Balance(noHint, parts...)
	if again := b.Balance(noHint, parts...); again != first {
		t.Errorf("fallback not deterministic: %d vs %d", first, again)
	}
	if first < 0 || first >= len(parts) {
		t.Errorf("fallback partition %d out of range [0,%d)", first, len(parts))
	}
}

// TestBuildWorkloadMessagesCarriesWorkerIndexHint performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
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

// TestValidateWorkerCapacity performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
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

// TestHashBalancerRoutesSameKeyToSamePartition performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestHashBalancerRoutesSameKeyToSamePartition(t *testing.T) {
	var b kafka.Hash
	parts := []int{0, 1, 2, 3, 4, 5}

	a := b.Balance(kafka.Message{Key: []byte("session-A")}, parts...)
	aAgain := b.Balance(kafka.Message{Key: []byte("session-A")}, parts...)
	if a != aAgain {
		t.Fatalf("same key routed to different partitions: %d vs %d", a, aAgain)
	}

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
