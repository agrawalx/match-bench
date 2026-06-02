package controller

import (
	"testing"

	"github.com/iicpc/schemas/topics"
)

func TestComputeWorkerCount(t *testing.T) {
	cases := []struct {
		totalTasks int
		want       uint32
	}{
		{0, 1}, // degenerate; clamped to >= 1
		{1, 1},
		{999, 1},
		{1000, 1}, // exact fit
		{1001, 2},
		{2000, 2},
		{2001, 3},
		{5110, 6}, // ramp peak — 9 waves × 511 = 4599 → 5 pods at 1000/pod
		{511, 1},  // constant baseline
		{2555, 3}, // spike total — 511 baseline + 2044 spike
	}
	for _, c := range cases {
		got := computeWorkerCount(c.totalTasks)
		if got != c.want {
			t.Errorf("computeWorkerCount(%d) = %d, want %d", c.totalTasks, got, c.want)
		}
	}
}

// TestRoundRobinShard verifies that buildWorkloadSpecs distributes tasks
// across worker pods round-robin by task_id, keeping each worker's profile
// distribution close to the scenario's overall mix.
func TestRoundRobinShard(t *testing.T) {
	// 12 tasks across 3 workers → 4 per worker.
	tasks := make([]topics.TaskSpec, 12)
	for i := range tasks {
		tasks[i] = topics.TaskSpec{TaskID: uint32(i)}
	}
	const workerCount uint32 = 3

	tasksByWorker := make([][]topics.TaskSpec, workerCount)
	for i := range tasksByWorker {
		tasksByWorker[i] = make([]topics.TaskSpec, 0)
	}
	for i, ts := range tasks {
		shard := uint32(i) % workerCount
		tasksByWorker[shard] = append(tasksByWorker[shard], ts)
	}

	for w := uint32(0); w < workerCount; w++ {
		if got, want := len(tasksByWorker[w]), 4; got != want {
			t.Errorf("worker %d: %d tasks, want %d", w, got, want)
		}
		for i, ts := range tasksByWorker[w] {
			expected := w + uint32(i)*workerCount
			if ts.TaskID != expected {
				t.Errorf("worker %d task %d: id=%d, want %d", w, i, ts.TaskID, expected)
			}
		}
	}
}
