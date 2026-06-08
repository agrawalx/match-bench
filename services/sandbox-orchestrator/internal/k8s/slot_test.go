package k8s

import (
	"testing"

	"github.com/iicpc/sandbox-orchestrator/internal/store"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// deriveState is the only piece of subtle logic in the orchestrator;
// these tests cover the failure-vs-creating transitions that the slot
// lifecycle depends on. The controller polls GET /slots/{id} and treats
// state=ready as the signal to publish workload.assignments; if
// deriveState returned "creating" for a pod that was actually stuck
// (e.g. ImagePullBackOff), the controller would wait the full
// DEPLOY_DEADLINE before failing the run rather than failing fast.
func TestDeriveState(t *testing.T) {
	cases := []struct {
		name      string
		pod       *corev1.Pod
		wantState store.SlotState
	}{
		{
			name:      "pending without containers is creating",
			pod:       &corev1.Pod{Status: corev1.PodStatus{Phase: corev1.PodPending}},
			wantState: store.StateCreating,
		},
		{
			name: "deletion timestamp is terminating",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					DeletionTimestamp: &metav1.Time{},
				},
				Status: corev1.PodStatus{Phase: corev1.PodRunning},
			},
			wantState: store.StateTerminating,
		},
		{
			name: "image pull backoff is failed",
			pod: &corev1.Pod{
				Status: corev1.PodStatus{
					Phase: corev1.PodPending,
					ContainerStatuses: []corev1.ContainerStatus{{
						Name: "algo",
						State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{
							Reason: "ImagePullBackOff", Message: "cannot pull",
						}},
					}},
				},
			},
			wantState: store.StateFailed,
		},
		{
			name: "create container config error is failed",
			pod: &corev1.Pod{
				Status: corev1.PodStatus{
					Phase: corev1.PodPending,
					ContainerStatuses: []corev1.ContainerStatus{{
						Name: "algo",
						State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{
							Reason: "CreateContainerConfigError",
						}},
					}},
				},
			},
			wantState: store.StateFailed,
		},
		{
			name: "running but not ready is creating",
			pod: &corev1.Pod{
				Status: corev1.PodStatus{
					Phase: corev1.PodRunning,
					Conditions: []corev1.PodCondition{
						{Type: corev1.PodReady, Status: corev1.ConditionFalse},
					},
				},
			},
			wantState: store.StateCreating,
		},
		{
			name: "running and ready is ready",
			pod: &corev1.Pod{
				Status: corev1.PodStatus{
					Phase: corev1.PodRunning,
					Conditions: []corev1.PodCondition{
						{Type: corev1.PodReady, Status: corev1.ConditionTrue},
					},
				},
			},
			wantState: store.StateReady,
		},
		{
			name: "pod failed phase is failed",
			pod: &corev1.Pod{
				Status: corev1.PodStatus{Phase: corev1.PodFailed, Message: "oom-killed"},
			},
			wantState: store.StateFailed,
		},
		{
			name: "pod succeeded with restart-never is failed (unexpected exit)",
			pod: &corev1.Pod{
				Status: corev1.PodStatus{Phase: corev1.PodSucceeded},
			},
			wantState: store.StateFailed,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, _ := deriveState(tc.pod)
			if got != tc.wantState {
				t.Errorf("got %s, want %s", got, tc.wantState)
			}
		})
	}
}

func TestValidateConfigRejectsMillicpu(t *testing.T) {
	err := validateConfig(Config{Namespace: "sandbox", CPU: "2000m", Memory: "1Gi"})
	if err == nil {
		t.Fatal("expected millicpu CPU to be rejected")
	}
}

func TestValidateConfigRejectsInvalidMemory(t *testing.T) {
	err := validateConfig(Config{Namespace: "sandbox", CPU: "2", Memory: "not-memory"})
	if err == nil {
		t.Fatal("expected invalid memory to be rejected")
	}
}

func TestPodNameAndFQDN(t *testing.T) {
	if podName("sess-123") != "algo-sess-123" {
		t.Errorf("podName: got %s", podName("sess-123"))
	}
	want := "algo-sess-123.sandbox.svc.cluster.local"
	if got := ServiceFQDN("sess-123", "sandbox"); got != want {
		t.Errorf("ServiceFQDN: got %s want %s", got, want)
	}
}

func TestPodSpecRuntimeClassOptional(t *testing.T) {
	mgr := &Manager{namespace: "sandbox", cpu: "2", memory: "1Gi"}
	pod := mgr.podSpec("s1", "", "img:tag", 8080)
	if pod.Spec.RuntimeClassName != nil {
		t.Errorf("expected no runtime class when env unset, got %v", *pod.Spec.RuntimeClassName)
	}

	mgr.runtimeClass = "gvisor"
	pod = mgr.podSpec("s1", "", "img:tag", 8080)
	if pod.Spec.RuntimeClassName == nil || *pod.Spec.RuntimeClassName != "gvisor" {
		t.Errorf("expected gvisor runtime class, got %v", pod.Spec.RuntimeClassName)
	}
}

func TestPodSpecImagePullSecretOptional(t *testing.T) {
	mgr := &Manager{namespace: "sandbox", cpu: "2", memory: "1Gi"}
	pod := mgr.podSpec("s1", "", "ghcr.io/iicpc/submission:latest", 8080)
	if len(pod.Spec.ImagePullSecrets) != 0 {
		t.Fatalf("expected no imagePullSecrets when env unset, got %v", pod.Spec.ImagePullSecrets)
	}

	mgr.imagePullSecretName = "registry-credentials"
	pod = mgr.podSpec("s1", "", "ghcr.io/iicpc/submission:latest", 8080)
	if got := pod.Spec.ImagePullSecrets; len(got) != 1 || got[0].Name != "registry-credentials" {
		t.Fatalf("imagePullSecrets = %v, want registry-credentials", got)
	}
}

func TestPodSpecLabels(t *testing.T) {
	mgr := &Manager{namespace: "sandbox", cpu: "2", memory: "1Gi"}
	pod := mgr.podSpec("sess-AAA", "", "img:tag", 8080)
	if pod.Labels[LabelApp] != AppValue {
		t.Errorf("missing app label: %v", pod.Labels)
	}
	if pod.Labels[LabelSlot] != "sess-AAA" {
		t.Errorf("missing slot label: %v", pod.Labels)
	}
	if pod.Labels[LabelManagedBy] != ManagedByValue {
		t.Errorf("missing managed-by label: %v", pod.Labels)
	}
	if pod.ObjectMeta.Name != "algo-sess-AAA" {
		t.Errorf("pod name: %s", pod.ObjectMeta.Name)
	}
}

// TestGuaranteedQoSShape locks the resource shape that fairness depends on.
//
// Kubernetes assigns Guaranteed QoS only when, for every container, every
// resource has request == limit AND CPU is specified as an integer count
// (not millicores). Burstable or BestEffort QoS makes algo pods eligible
// for kubelet CPU throttling under contention — fatal for HFT-style p99
// claims. Guaranteed QoS is also the precondition for the kubelet CPU
// manager (when configured with cpuManagerPolicy=static) to give the pod
// exclusive cpuset pinning; without that, two pods can share the same
// physical cores even though they each "own" 2 CPUs.
func TestGuaranteedQoSShape(t *testing.T) {
	mgr := &Manager{namespace: "sandbox", cpu: "2", memory: "1Gi"}
	pod := mgr.podSpec("s1", "", "img:tag", 8080)

	c := pod.Spec.Containers[0]
	cpuReq := c.Resources.Requests[corev1.ResourceCPU]
	cpuLim := c.Resources.Limits[corev1.ResourceCPU]
	memReq := c.Resources.Requests[corev1.ResourceMemory]
	memLim := c.Resources.Limits[corev1.ResourceMemory]

	if cpuReq.Cmp(cpuLim) != 0 {
		t.Errorf("cpu request (%s) != limit (%s) — pod would be Burstable, breaks cpuset pinning", &cpuReq, &cpuLim)
	}
	if memReq.Cmp(memLim) != 0 {
		t.Errorf("memory request (%s) != limit (%s) — pod would be Burstable", &memReq, &memLim)
	}
}

// TestReadOnlyRootAndTmpfsMounts: disk-I/O fairness depends on BOTH the
// readOnlyRootFilesystem flag AND tmpfs emptyDirs at every writable path.
// Either alone is incomplete.
func TestReadOnlyRootAndTmpfsMounts(t *testing.T) {
	mgr := &Manager{namespace: "sandbox", cpu: "2", memory: "1Gi"}
	pod := mgr.podSpec("s1", "", "img:tag", 8080)

	c := pod.Spec.Containers[0]
	if c.SecurityContext == nil || c.SecurityContext.ReadOnlyRootFilesystem == nil || !*c.SecurityContext.ReadOnlyRootFilesystem {
		t.Errorf("readOnlyRootFilesystem must be true — without it contestants can write to the container's overlay fs")
	}

	wantMounts := map[string]string{
		"tmp":     "/tmp",
		"var-tmp": "/var/tmp",
		"var-log": "/var/log",
		"var-run": "/var/run",
	}
	got := map[string]string{}
	for _, m := range c.VolumeMounts {
		got[m.Name] = m.MountPath
	}
	for name, path := range wantMounts {
		if got[name] != path {
			t.Errorf("missing tmpfs mount: name=%s path=%s (got %v)", name, path, got)
		}
	}

	for _, v := range pod.Spec.Volumes {
		if v.EmptyDir == nil || v.EmptyDir.Medium != corev1.StorageMediumMemory {
			t.Errorf("volume %s is not tmpfs — disk I/O isolation broken", v.Name)
		}
	}
}

func TestPodSpecNodePoolPinning(t *testing.T) {
	mgr := &Manager{namespace: "sandbox", cpu: "2", memory: "1Gi"}
	pod := mgr.podSpec("s1", "", "img:tag", 8080)
	if len(pod.Spec.Tolerations) != 0 || pod.Spec.NodeSelector != nil {
		t.Errorf("expected no node pinning when SANDBOX_NODE_POOL unset")
	}

	mgr.nodePool = "sandbox"
	pod = mgr.podSpec("s1", "", "img:tag", 8080)
	if pod.Spec.NodeSelector["pool"] != "sandbox" {
		t.Errorf("expected pool=sandbox nodeSelector, got %v", pod.Spec.NodeSelector)
	}
	if len(pod.Spec.Tolerations) != 1 || pod.Spec.Tolerations[0].Key != "sandbox" {
		t.Errorf("expected sandbox=true:NoSchedule toleration, got %v", pod.Spec.Tolerations)
	}
}

func TestBandwidthAnnotations(t *testing.T) {
	mgr := &Manager{namespace: "sandbox", cpu: "2", memory: "1Gi"}
	pod := mgr.podSpec("s1", "", "img:tag", 8080)
	if _, ok := pod.Annotations["kubernetes.io/egress-bandwidth"]; ok {
		t.Errorf("expected no bandwidth annotation when env unset")
	}

	mgr.egressBwBps = "100M"
	mgr.ingressBwBps = "50M"
	pod = mgr.podSpec("s1", "", "img:tag", 8080)
	if pod.Annotations["kubernetes.io/egress-bandwidth"] != "100M" {
		t.Errorf("egress bandwidth annotation not set")
	}
	if pod.Annotations["kubernetes.io/ingress-bandwidth"] != "50M" {
		t.Errorf("ingress bandwidth annotation not set")
	}
}

// Compile-time check: metav1 is used through ObjectMeta even without direct
// imports in tests, but keep the import warm here for future test additions.
var _ = metav1.ObjectMeta{}
