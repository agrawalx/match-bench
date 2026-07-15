package controller

import (
	"testing"

	"github.com/iicpc/bot-fleet-controller/internal/orchestrator"
	"github.com/iicpc/bot-fleet-controller/internal/store"
	"github.com/iicpc/schemas/topics"
)

func taskSpecs(n int) []topics.TaskSpec {
	specs := make([]topics.TaskSpec, n)
	for i := range specs {
		specs[i] = topics.TaskSpec{TaskID: uint32(i), TargetRPS: 100}
	}
	return specs
}

func TestSubmissionTargetsSingleProtocol(t *testing.T) {
	sub := &store.SubmissionInfo{Protocol: "FIX"}
	targets := submissionTargets(sub)
	if len(targets) != 1 || targets[0].Protocol != "FIX" || targets[0].Port != topics.PortFIX {
		t.Fatalf("unexpected targets: %+v", targets)
	}
}

func TestSubmissionTargetsAllProtocols(t *testing.T) {
	sub := &store.SubmissionInfo{Protocol: protocolAll}
	targets := submissionTargets(sub)
	if len(targets) != 3 {
		t.Fatalf("expected 3 targets, got %d: %+v", len(targets), targets)
	}
	want := map[string]uint16{"FIX": topics.PortFIX, "REST": topics.PortHTTPWS, "WS": topics.PortHTTPWS}
	for _, tg := range targets {
		if want[tg.Protocol] != tg.Port {
			t.Fatalf("unexpected port for %s: got %d want %d", tg.Protocol, tg.Port, want[tg.Protocol])
		}
	}
}

func TestBuildWorkloadSpecsSplitsTasksRoundRobinAcrossTargets(t *testing.T) {
	r := &Runner{runConfig: RunConfig{GlobalSeed: 1}}
	sess := &Session{
		SessionID:    "sess-1",
		SubmissionID: "sub-1",
		Endpoint:     &orchestrator.Endpoint{Host: "algo.svc", Port: 9898},
	}
	sub := &store.SubmissionInfo{ContestantID: "team-1", Protocol: protocolAll}
	scenario := &topics.Scenario{TaskSpecs: taskSpecs(9)}

	specs := r.buildWorkloadSpecs(sess, sub, scenario, 2)
	if len(specs) != 2 {
		t.Fatalf("expected 2 worker specs, got %d", len(specs))
	}

	seen := map[uint32]uint8{}
	total := 0
	for _, spec := range specs {
		if len(spec.Targets) != 3 {
			t.Fatalf("expected 3 targets on every spec, got %d", len(spec.Targets))
		}
		for _, ts := range spec.Tasks {
			seen[ts.TaskID] = ts.TargetIdx
			total++
		}
	}
	if total != 9 {
		t.Fatalf("expected all 9 tasks distributed, got %d", total)
	}
	// task_id i must land on target_idx i%3, independent of worker sharding.
	for taskID, idx := range seen {
		want := uint8(taskID % 3)
		if idx != want {
			t.Errorf("task %d: got target_idx %d, want %d", taskID, idx, want)
		}
	}
}

func TestBuildWorkloadSpecsKeepsTaskIDsGloballyUnique(t *testing.T) {
	r := &Runner{runConfig: RunConfig{}}
	sess := &Session{
		SessionID:    "sess-1",
		SubmissionID: "sub-1",
		Endpoint:     &orchestrator.Endpoint{Host: "algo.svc", Port: 9898},
	}
	sub := &store.SubmissionInfo{Protocol: protocolAll}
	scenario := &topics.Scenario{TaskSpecs: taskSpecs(12)}

	specs := r.buildWorkloadSpecs(sess, sub, scenario, 3)
	ids := map[uint32]bool{}
	for _, spec := range specs {
		for _, ts := range spec.Tasks {
			if ids[ts.TaskID] {
				t.Fatalf("duplicate task_id %d across workers", ts.TaskID)
			}
			ids[ts.TaskID] = true
		}
	}
	if len(ids) != 12 {
		t.Fatalf("expected 12 unique task_ids, got %d", len(ids))
	}
}
