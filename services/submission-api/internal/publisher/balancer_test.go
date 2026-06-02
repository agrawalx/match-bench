package publisher

import (
	"log/slog"
	"testing"

	kafka "github.com/segmentio/kafka-go"
)

// TestKeyedWritersUseHashBalancer reproduces M19: both writers set a per-message
// Key (submission_id / session_id) but used kafka.LeastBytes, which ignores the
// Key and scatters same-key messages across partitions — breaking per-key ordering
// and the documented session partitioning. They must use a key-aware balancer.
func TestKeyedWritersUseHashBalancer(t *testing.T) {
	p := NewKafkaPublisher("localhost:9092", slog.Default())
	defer p.Close()

	if _, ok := p.buildWriter.Balancer.(*kafka.Hash); !ok {
		t.Errorf("buildWriter.Balancer = %T, want *kafka.Hash (keyed by submission_id)", p.buildWriter.Balancer)
	}
	if _, ok := p.benchmarkWriter.Balancer.(*kafka.Hash); !ok {
		t.Errorf("benchmarkWriter.Balancer = %T, want *kafka.Hash (keyed by session_id)", p.benchmarkWriter.Balancer)
	}
}
