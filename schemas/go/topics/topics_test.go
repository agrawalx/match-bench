package topics

import (
	"encoding/json"
	"testing"
)

func TestWorkloadSpecDecodesLegacySinglePayload(t *testing.T) {
	payload := []byte(`{
		"session_id":"sess-1",
		"submission_id":"sub-1",
		"target_host":"algo-sess-1.sandbox.svc.cluster.local",
		"target_port":9898,
		"protocol":"FIX",
		"worker_index":0,
		"worker_count":1,
		"global_seed":42,
		"tasks":[
			{"task_id":1,"profile":"hft","target_rps":50,"start_offset_ns":0,"duration_ns":1000000000}
		]
	}`)

	var spec WorkloadSpec
	if err := json.Unmarshal(payload, &spec); err != nil {
		t.Fatalf("decode workload spec: %v", err)
	}
	if len(spec.Targets) != 0 {
		t.Fatalf("expected no targets on legacy payload, got %v", spec.Targets)
	}
	if spec.Tasks[0].TargetIdx != 0 {
		t.Fatalf("expected default target_idx 0, got %d", spec.Tasks[0].TargetIdx)
	}
	resolved := spec.ResolvedTargets()
	if len(resolved) != 1 || resolved[0].Protocol != "FIX" || resolved[0].Port != 9898 {
		t.Fatalf("unexpected resolved targets: %+v", resolved)
	}
}

func TestWorkloadSpecDecodesMultiTargetPayload(t *testing.T) {
	payload := []byte(`{
		"session_id":"sess-1",
		"submission_id":"sub-1",
		"target_host":"algo-sess-1.sandbox.svc.cluster.local",
		"target_port":9898,
		"protocol":"FIX",
		"targets":[
			{"protocol":"FIX","port":9898},
			{"protocol":"REST","port":8080},
			{"protocol":"WS","port":8080}
		],
		"worker_index":0,
		"worker_count":1,
		"global_seed":42,
		"tasks":[
			{"task_id":1,"profile":"hft","target_rps":50,"start_offset_ns":0,"duration_ns":1000000000,"target_idx":2}
		]
	}`)

	var spec WorkloadSpec
	if err := json.Unmarshal(payload, &spec); err != nil {
		t.Fatalf("decode workload spec: %v", err)
	}
	if len(spec.Targets) != 3 {
		t.Fatalf("expected 3 targets, got %d", len(spec.Targets))
	}
	if spec.Tasks[0].TargetIdx != 2 {
		t.Fatalf("expected target_idx 2, got %d", spec.Tasks[0].TargetIdx)
	}
	resolved := spec.ResolvedTargets()
	got := resolved[spec.Tasks[0].TargetIdx]
	if got.Protocol != "WS" || got.Port != 8080 {
		t.Fatalf("unexpected resolved target: %+v", got)
	}
}

func TestPortForProtocolMatchesPlatformPolicy(t *testing.T) {
	if PortForProtocol("FIX") != PortFIX {
		t.Fatalf("FIX port mismatch")
	}
	if PortForProtocol("REST") != PortHTTPWS {
		t.Fatalf("REST port mismatch")
	}
	if PortForProtocol("WS") != PortHTTPWS {
		t.Fatalf("WS port mismatch")
	}
	if PortFIX != 9898 || PortHTTPWS != 8080 {
		t.Fatalf("platform port constants drifted")
	}
}
