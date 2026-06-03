package k8s

import (
	"context"
	"errors"
	"testing"

	cerrs "github.com/iicpc/sandbox-orchestrator/internal/errors"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func bugfixManager(captureEnabled bool) *Manager {
	return &Manager{
		client:         fake.NewSimpleClientset(),
		namespace:      "sandbox",
		cpu:            "2",
		memory:         "1Gi",
		captureEnabled: captureEnabled,
		captureImage:   "capture:dev",
		kafkaBrokers:   "kafka:9092",
	}
}

// H9: the untrusted algo container must be hardened (no priv-esc, drop all caps,
// runtime-default seccomp) without forcing RunAsNonRoot (which would break
// legitimate root contestant images).
func TestAlgoPodHardened(t *testing.T) {
	sc := bugfixManager(false).podSpec("s1", "c1", "img", 9898).Spec.Containers[0].SecurityContext
	if sc == nil {
		t.Fatal("algo container has no SecurityContext")
	}
	if sc.AllowPrivilegeEscalation == nil || *sc.AllowPrivilegeEscalation {
		t.Error("AllowPrivilegeEscalation must be false")
	}
	if sc.Capabilities == nil || len(sc.Capabilities.Drop) == 0 || sc.Capabilities.Drop[0] != "ALL" {
		t.Errorf("capabilities must drop ALL, got %+v", sc.Capabilities)
	}
	if sc.SeccompProfile == nil || sc.SeccompProfile.Type != corev1.SeccompProfileTypeRuntimeDefault {
		t.Error("seccomp profile must be RuntimeDefault")
	}
	if sc.RunAsNonRoot != nil && *sc.RunAsNonRoot {
		t.Error("must NOT force RunAsNonRoot on arbitrary contestant images")
	}
}

// H8: the algo pod must carry an ActiveDeadlineSeconds backstop so a leaked pod
// self-terminates instead of holding a node forever.
func TestAlgoPodHasActiveDeadline(t *testing.T) {
	pod := bugfixManager(false).podSpec("s1", "c1", "img", 9898)
	if pod.Spec.ActiveDeadlineSeconds == nil || *pod.Spec.ActiveDeadlineSeconds <= 0 {
		t.Fatalf("algo pod missing ActiveDeadlineSeconds backstop: %v", pod.Spec.ActiveDeadlineSeconds)
	}
}

// M22: with capture enabled, a slot on a port the BPF program does not match must
// be rejected (HTTP 400 via ErrInvalidRequest), not come up silently captureless.
func TestCreateSlotRejectsUncapturablePort(t *testing.T) {
	ctx := context.Background()
	m := bugfixManager(true)
	if err := m.CreateSlot(ctx, "s-bad", "c1", "img", 1234); !errors.Is(err, cerrs.ErrInvalidRequest) {
		t.Fatalf("port 1234 (capture on): got %v, want ErrInvalidRequest", err)
	}
	// A capturable port must not be rejected on port grounds.
	if err := m.CreateSlot(ctx, "s-ok", "c1", "img", 9898); errors.Is(err, cerrs.ErrInvalidRequest) {
		t.Fatalf("port 9898 (capture on) wrongly rejected: %v", err)
	}
	// Capture disabled: any port is fine.
	if err := bugfixManager(false).CreateSlot(ctx, "s-any", "c1", "img", 1234); errors.Is(err, cerrs.ErrInvalidRequest) {
		t.Fatalf("port 1234 (capture off) wrongly rejected: %v", err)
	}
}

// M23: the capture Job must have an ActiveDeadlineSeconds bound and an
// ownerReference to the algo pod (so it is GC'd on pod deletion), since its
// infinite-loop process never lets TTLSecondsAfterFinished fire.
func TestCaptureJobBoundedAndOwned(t *testing.T) {
	job := bugfixManager(true).captureJobSpec("s1", "c1", "node-1", "pod-uid-123", "containerd://abc")
	if job.Spec.ActiveDeadlineSeconds == nil || *job.Spec.ActiveDeadlineSeconds <= 0 {
		t.Error("capture Job missing ActiveDeadlineSeconds")
	}
	if len(job.OwnerReferences) == 0 {
		t.Fatal("capture Job missing ownerReference to the algo pod")
	}
	ref := job.OwnerReferences[0]
	if ref.Kind != "Pod" || string(ref.UID) != "pod-uid-123" {
		t.Errorf("ownerReference = %+v, want Pod/pod-uid-123", ref)
	}
}
