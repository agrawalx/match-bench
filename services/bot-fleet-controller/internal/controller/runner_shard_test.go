// Package controller defines tests for runner shard test.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package controller

import (
	"testing"

	"github.com/iicpc/schemas/topics"
)

// TestComputeWorkerCount performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
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
		got := computeWorkerCount(c.totalTasks, DefaultMaxTasksPerWorker)
		if got != c.want {
			t.Errorf("computeWorkerCount(%d) = %d, want %d", c.totalTasks, got, c.want)
		}
	}

	if got := computeWorkerCount(2555, 100000); got != 1 {
		t.Errorf("pin-to-one-pod: computeWorkerCount(2555, 100000) = %d, want 1", got)
	}
	if got := computeWorkerCount(2555, 511); got != 5 {
		t.Errorf("fan-out: computeWorkerCount(2555, 511) = %d, want 5", got)
	}
	if got := computeWorkerCount(500, 0); got != 1 { // 0 falls back to default 1000
		t.Errorf("zero ceiling falls back to default: got %d, want 1", got)
	}
}

// TestRoundRobinShard performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestRoundRobinShard(t *testing.T) {
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
