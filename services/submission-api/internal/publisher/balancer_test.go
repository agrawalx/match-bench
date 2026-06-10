package publisher

import (
	"log/slog"
	"testing"

	kafka "github.com/segmentio/kafka-go"
)

// TestKeyedWritersUseHashBalancer reproduces M19: both writers set a per-message
// Key (submission_id / run_group_id) but used kafka.LeastBytes, which ignores the
// Key and scatters same-key messages across partitions — breaking per-key ordering
// and the documented session partitioning. They must use a key-aware balancer.
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

// TestBenchmarkMessageKey pins the benchmark.requested partition key to
// run_group_id. Keyed by session_id, one click's three sibling sessions
// hashed onto up to three partitions of the 3-partition topic and raced each
// other through the controller; keyed by run_group_id they serialize on ONE
// partition in publish order. Legacy single-session messages (no group) fall
// back to session_id so they keep a stable, non-empty key.
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
