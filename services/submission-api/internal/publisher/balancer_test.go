// Package publisher defines tests for balancer test.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package publisher

import (
	"log/slog"
	"testing"

	kafka "github.com/segmentio/kafka-go"
)

// TestKeyedWritersUseHashBalancer performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestKeyedWritersUseHashBalancer(t *testing.T) {
	p := NewKafkaPublisher("localhost:9092", slog.Default())
	defer p.Close()

	if _, ok := p.buildWriter.Balancer.(*kafka.Hash); !ok {
		t.Errorf("buildWriter.Balancer = %T, want *kafka.Hash (keyed by submission_id)", p.buildWriter.Balancer)
	}
	if _, ok := p.benchmarkWriter.Balancer.(*kafka.Hash); !ok {
		t.Errorf("benchmarkWriter.Balancer = %T, want *kafka.Hash (keyed by run_group_id)", p.benchmarkWriter.Balancer)
	}
}

// TestBenchmarkMessageKey performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestBenchmarkMessageKey(t *testing.T) {
	tests := []struct {
		name string
		meta BenchmarkMeta
		want string
	}{
		{
			name: "grouped message keys on run_group_id",
			meta: BenchmarkMeta{SessionID: "sess-1", RunGroupID: "group-1"},
			want: "group-1",
		},
		{
			name: "sibling sessions share the group key",
			meta: BenchmarkMeta{SessionID: "sess-2", RunGroupID: "group-1"},
			want: "group-1",
		},
		{
			name: "legacy ungrouped message falls back to session_id",
			meta: BenchmarkMeta{SessionID: "sess-legacy"},
			want: "sess-legacy",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := string(benchmarkMessageKey(tt.meta)); got != tt.want {
				t.Errorf("benchmarkMessageKey(%+v) = %q, want %q", tt.meta, got, tt.want)
			}
		})
	}
}
