package config

import (
	"os"
	"testing"
)

// TestSSEConsumerGroupPerPod guards the SSE fan-out posture: every replica
// must land in its own consumer group, otherwise replicas split the
// leaderboard.updates partitions and only a subset of SSE clients see updates.
func TestSSEConsumerGroupPerPod(t *testing.T) {
	cases := []struct {
		name    string
		podName string
		group   string
		want    string
	}{
		{name: "downward_api_pod_name", podName: "leaderboard-api-7f9c-x2x", want: "leaderboard-api-sse-leaderboard-api-7f9c-x2x"},
		{name: "kafka_group_override_keeps_pod_suffix", podName: "pod-1", group: "custom-base", want: "custom-base-pod-1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("POD_NAME", tc.podName)
			t.Setenv("KAFKA_GROUP", tc.group)
			if got := sseConsumerGroup(); got != tc.want {
				t.Fatalf("sseConsumerGroup() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestSSEConsumerGroupFallsBackToHostname(t *testing.T) {
	t.Setenv("POD_NAME", "")
	t.Setenv("KAFKA_GROUP", "")
	host, err := os.Hostname()
	if err != nil {
		t.Skipf("hostname unavailable: %v", err)
	}
	want := "leaderboard-api-sse-" + host
	if got := sseConsumerGroup(); got != want {
		t.Fatalf("sseConsumerGroup() = %q, want %q", got, want)
	}
}
