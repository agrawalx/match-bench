package k8s

import (
	"context"
	"fmt"
	"strings"

	cerrs "github.com/iicpc/sandbox-orchestrator/internal/errors"
	"github.com/iicpc/sandbox-orchestrator/internal/store"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
)

// Labels applied to every slot resource so the orchestrator can list its own
// resources without colliding with anything else in the sandbox namespace.
const (
	LabelApp       = "app"
	LabelSlot      = "slot"
	LabelManagedBy = "app.kubernetes.io/managed-by"

	AppValue       = "algo"
	ManagedByValue = "sandbox-orchestrator"
)

// Manager wraps k8s client operations for sandbox slot lifecycle.
// Pod + Service are always created and deleted as a pair — one slot
// is conceptually both resources (see CONVENTIONS.md §10).
//
// All pod-spec fields here are designed to make the benchmark FAIR across
// contestants. See SANDBOX_FAIRNESS.md for the full rationale and the
// kubelet-level / OS-level pieces that this pod spec depends on.
type Manager struct {
	client       kubernetes.Interface
	namespace    string
	runtimeClass string // empty → no runtimeClassName set; matches BUILD_NODE_POOL toggle pattern
	cpu          string // integer CPU count as a string, e.g. "2". Same value used for request AND limit (Guaranteed QoS).
	memory       string // memory size with unit, e.g. "1Gi". Same value used for request AND limit.
	nodePool     string // empty → no nodeAffinity/toleration; non-empty → pin to that node pool
	egressBwBps  string // empty → no annotation; non-empty → CNI bandwidth plugin throttles egress (e.g. "100M")
	ingressBwBps string // same for ingress
}

type Config struct {
	Namespace    string
	RuntimeClass string // e.g. "gvisor" in prod; empty in dev k3s
	// CPU is the integer CPU count (string) for both request and limit.
	// Must be an integer string ("1", "2", "4") for cpuset pinning to work —
	// the kubelet's CPU manager (static policy) only pins integer-CPU
	// Guaranteed-QoS pods. See SANDBOX_FAIRNESS.md Tier 1+2.
	CPU string
	// Memory is the size string (e.g. "1Gi") for both request and limit.
	// Equal request/limit makes the pod Guaranteed QoS — required for
	// fair memory.max enforcement and (with kubelet config) cpuset pinning.
	Memory string
	// NodePool, when non-empty, pins algo pods to nodes labelled
	// pool=<value> with toleration for taint sandbox=true:NoSchedule.
	// Mirrors BUILD_NODE_POOL pattern. Empty in dev k3s.
	NodePool string
	// Egress/IngressBandwidth, when non-empty, are CNI bandwidth-plugin
	// annotations applied to the pod (e.g. "100M"). Cilium also honors
	// these as kubernetes.io/egress-bandwidth.
	EgressBandwidth  string
	IngressBandwidth string
}

// Namespace returns the sandbox namespace this manager operates in.
// Used by handlers to build Service FQDNs without duplicating the env value.
func (m *Manager) Namespace() string { return m.namespace }

func NewManager(client kubernetes.Interface, cfg Config) *Manager {
	return &Manager{
		client:       client,
		namespace:    cfg.Namespace,
		runtimeClass: cfg.RuntimeClass,
		cpu:          cfg.CPU,
		memory:       cfg.Memory,
		nodePool:     cfg.NodePool,
		egressBwBps:  cfg.EgressBandwidth,
		ingressBwBps: cfg.IngressBandwidth,
	}
}

// CreateSlot creates the Pod + Service pair for a slot.
// Idempotent: if both resources already exist with matching image, returns
// nil (caller observes via Refresh). If a resource exists with a different
// image, returns ErrSlotImageMismatch.
func (m *Manager) CreateSlot(ctx context.Context, slotID, image string, port int) error {
	resourceName := podName(slotID)

	existing, err := m.client.CoreV1().Pods(m.namespace).Get(ctx, resourceName, metav1.GetOptions{})
	switch {
	case err == nil:
		// Pod exists — verify image matches.
		if len(existing.Spec.Containers) == 0 || existing.Spec.Containers[0].Image != image {
			return fmt.Errorf("%w: existing pod image %q", cerrs.ErrSlotImageMismatch, existing.Spec.Containers[0].Image)
		}
	case apierrors.IsNotFound(err):
		// Expected path: create the Pod.
		if _, err := m.client.CoreV1().Pods(m.namespace).Create(ctx, m.podSpec(slotID, image, port), metav1.CreateOptions{}); err != nil {
			// Race: someone created the pod between our Get and Create.
			if apierrors.IsAlreadyExists(err) {
				return m.CreateSlot(ctx, slotID, image, port)
			}
			return fmt.Errorf("create pod: %w", err)
		}
	default:
		return fmt.Errorf("get pod: %w", err)
	}

	if _, err := m.client.CoreV1().Services(m.namespace).Get(ctx, resourceName, metav1.GetOptions{}); err != nil {
		if !apierrors.IsNotFound(err) {
			return fmt.Errorf("get service: %w", err)
		}
		if _, err := m.client.CoreV1().Services(m.namespace).Create(ctx, m.serviceSpec(slotID, port), metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("create service: %w", err)
		}
	}

	return nil
}

// DeleteSlot removes both the Pod and the Service for a slot.
// Returns nil if either resource is already gone — DELETE is idempotent.
func (m *Manager) DeleteSlot(ctx context.Context, slotID string) error {
	resourceName := podName(slotID)

	if err := m.client.CoreV1().Pods(m.namespace).Delete(ctx, resourceName, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete pod: %w", err)
	}
	if err := m.client.CoreV1().Services(m.namespace).Delete(ctx, resourceName, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete service: %w", err)
	}
	return nil
}

// Refresh reads the live Pod state from k8s and returns the derived SlotState.
// Returns ErrSlotNotFound when the Pod no longer exists.
func (m *Manager) Refresh(ctx context.Context, slotID string) (store.SlotState, string, error) {
	pod, err := m.client.CoreV1().Pods(m.namespace).Get(ctx, podName(slotID), metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return "", "", cerrs.ErrSlotNotFound
		}
		return "", "", fmt.Errorf("get pod: %w", err)
	}
	state, msg := deriveState(pod)
	return state, msg, nil
}

// ListExisting enumerates slots already in the cluster on startup so the
// in-memory map can be rebuilt from k8s. k8s is the durable source of truth
// (CONVENTIONS.md §10) — anything we miss here is leaked or orphaned.
func (m *Manager) ListExisting(ctx context.Context) ([]store.Slot, error) {
	selector := fmt.Sprintf("%s=%s,%s=%s", LabelApp, AppValue, LabelManagedBy, ManagedByValue)
	pods, err := m.client.CoreV1().Pods(m.namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return nil, fmt.Errorf("list existing pods: %w", err)
	}

	out := make([]store.Slot, 0, len(pods.Items))
	for _, pod := range pods.Items {
		slotID := pod.Labels[LabelSlot]
		if slotID == "" {
			continue
		}
		image := ""
		port := 0
		if len(pod.Spec.Containers) > 0 {
			image = pod.Spec.Containers[0].Image
			if len(pod.Spec.Containers[0].Ports) > 0 {
				port = int(pod.Spec.Containers[0].Ports[0].ContainerPort)
			}
		}
		state, msg := deriveState(&pod)
		out = append(out, store.Slot{
			SlotID:    slotID,
			Image:     image,
			Port:      port,
			State:     state,
			Message:   msg,
			Endpoint:  store.Endpoint{Host: ServiceFQDN(slotID, m.namespace), Port: port},
			CreatedAt: pod.CreationTimestamp.Time,
		})
	}
	return out, nil
}

// podName returns the pod (and service) name for a slot.
// Convention: algo-{slot_id}. Both resources share this name.
func podName(slotID string) string {
	return "algo-" + slotID
}

// ServiceFQDN returns the cluster-internal DNS name a controller passes as
// target_host in WorkloadSpec.
func ServiceFQDN(slotID, namespace string) string {
	return fmt.Sprintf("%s.%s.svc.cluster.local", podName(slotID), namespace)
}

func (m *Manager) podSpec(slotID, image string, port int) *corev1.Pod {
	labels := map[string]string{
		LabelApp:       AppValue,
		LabelSlot:      slotID,
		LabelManagedBy: ManagedByValue,
	}

	// Bandwidth annotations: CNI bandwidth plugin / Cilium reads these and
	// applies tc qdisc throttling so one chatty contestant cannot saturate
	// the node NIC at the expense of neighbours. See SANDBOX_FAIRNESS.md Tier 1.
	annotations := map[string]string{}
	if m.egressBwBps != "" {
		annotations["kubernetes.io/egress-bandwidth"] = m.egressBwBps
	}
	if m.ingressBwBps != "" {
		annotations["kubernetes.io/ingress-bandwidth"] = m.ingressBwBps
	}

	autoMount := false
	// readOnlyRoot prevents writes to the container's overlayfs layer; combined
	// with tmpfs emptyDir mounts below, this eliminates ALL disk I/O from the
	// algo pod. See SANDBOX_FAIRNESS.md Tier 1 (disk I/O fairness).
	readOnlyRoot := true

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        podName(slotID),
			Namespace:   m.namespace,
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: corev1.PodSpec{
			RestartPolicy:                corev1.RestartPolicyNever,
			AutomountServiceAccountToken: &autoMount,
			// Four tmpfs (RAM-backed) emptyDirs at the paths contestant code
			// is most likely to write to. RAM usage counts against the pod's
			// memory limit, so a contestant that logs heavily eats into their
			// own allocation instead of hitting node disk.
			Volumes: writableVolumes(),
			Containers: []corev1.Container{{
				Name:  "algo",
				Image: image,
				Ports: []corev1.ContainerPort{{ContainerPort: int32(port), Protocol: corev1.ProtocolTCP}},
				ReadinessProbe: &corev1.Probe{
					ProbeHandler: corev1.ProbeHandler{
						TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt(port)},
					},
					InitialDelaySeconds: 1,
					PeriodSeconds:       1,
					FailureThreshold:    30,
				},
				Resources: m.containerResources(),
				SecurityContext: &corev1.SecurityContext{
					ReadOnlyRootFilesystem: &readOnlyRoot,
				},
				VolumeMounts: writableMounts(),
			}},
		},
	}

	if m.runtimeClass != "" {
		rc := m.runtimeClass
		pod.Spec.RuntimeClassName = &rc
	}

	// Node-pool pinning: dedicated sandbox node pool prevents platform
	// workloads from sharing CPU/memory with contestant pods. Mirrors the
	// BUILD_NODE_POOL pattern. See SANDBOX_FAIRNESS.md Tier 1.
	if m.nodePool != "" {
		pod.Spec.Tolerations = []corev1.Toleration{{
			Key:      "sandbox",
			Operator: corev1.TolerationOpEqual,
			Value:    "true",
			Effect:   corev1.TaintEffectNoSchedule,
		}}
		pod.Spec.NodeSelector = map[string]string{"pool": m.nodePool}
	}

	return pod
}

// writableVolumes returns the four tmpfs emptyDirs every algo pod mounts.
// Memory-backed → no disk I/O → counts against the pod's memory.max cgroup.
// SANDBOX_FAIRNESS.md Tier 1.
func writableVolumes() []corev1.Volume {
	tmpfs := func(name string) corev1.Volume {
		return corev1.Volume{
			Name: name,
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{Medium: corev1.StorageMediumMemory},
			},
		}
	}
	return []corev1.Volume{
		tmpfs("tmp"),
		tmpfs("var-tmp"),
		tmpfs("var-log"),
		tmpfs("var-run"),
	}
}

// writableMounts pairs with writableVolumes — the contestant container can
// only write to these four paths, and only as RAM (cgroup-accounted).
func writableMounts() []corev1.VolumeMount {
	return []corev1.VolumeMount{
		{Name: "tmp", MountPath: "/tmp"},
		{Name: "var-tmp", MountPath: "/var/tmp"},
		{Name: "var-log", MountPath: "/var/log"},
		{Name: "var-run", MountPath: "/var/run"},
	}
}

func (m *Manager) serviceSpec(slotID string, port int) *corev1.Service {
	labels := map[string]string{
		LabelApp:       AppValue,
		LabelSlot:      slotID,
		LabelManagedBy: ManagedByValue,
	}
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName(slotID),
			Namespace: m.namespace,
			Labels:    labels,
		},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{
				LabelApp:  AppValue,
				LabelSlot: slotID,
			},
			Ports: []corev1.ServicePort{{
				Port:       int32(port),
				TargetPort: intstr.FromInt(port),
				Protocol:   corev1.ProtocolTCP,
			}},
		},
	}
}

// containerResources builds a Guaranteed-QoS resource block: request == limit
// for both CPU and memory. This is the gate that unlocks:
//   - cgroup v2 memory.max == memory.high (no kernel reclamation under load)
//   - kubelet CPU manager (static policy) cpuset pinning, when the node is
//     configured for it
// See SANDBOX_FAIRNESS.md Tier 1+2.
//
// Both values must be parseable by resource.MustParse. For CPU pinning,
// the value must also be an integer ("1", "2", "4") — millicpu strings
// ("500m") would still be Guaranteed but the CPU manager would skip pinning.
func (m *Manager) containerResources() corev1.ResourceRequirements {
	list := corev1.ResourceList{}
	if m.cpu != "" {
		list[corev1.ResourceCPU] = resource.MustParse(m.cpu)
	}
	if m.memory != "" {
		list[corev1.ResourceMemory] = resource.MustParse(m.memory)
	}
	// Same map used for both — guaranteed QoS only triggers when request
	// and limit are bytewise equal, which copying the same ResourceList
	// satisfies.
	return corev1.ResourceRequirements{Requests: list.DeepCopy(), Limits: list.DeepCopy()}
}

// deriveState turns a live Pod into the orchestrator's SlotState + message.
// Terminal pull/config errors land in failed; transient creating phases stay
// in creating. See CONVENTIONS.md §10 for the lifecycle contract.
func deriveState(pod *corev1.Pod) (store.SlotState, string) {
	// Terminal phases first.
	switch pod.Status.Phase {
	case corev1.PodFailed:
		return store.StateFailed, fmt.Sprintf("pod failed: %s", pod.Status.Message)
	case corev1.PodSucceeded:
		// restartPolicy=Never + container exited 0 = contestant binary exited
		// unexpectedly. Treat as failed; the algo should be long-running.
		return store.StateFailed, "container exited unexpectedly (algo should stay running)"
	}

	// Inspect container states for terminal waiting reasons.
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.State.Waiting == nil {
			continue
		}
		reason := cs.State.Waiting.Reason
		if isTerminalWaitingReason(reason) {
			return store.StateFailed, fmt.Sprintf("container %s: %s — %s", cs.Name, reason, cs.State.Waiting.Message)
		}
	}

	// Ready when k8s PodReady condition is True.
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
			return store.StateReady, "ready"
		}
	}

	return store.StateCreating, "pod scheduling / readiness probe pending"
}

func isTerminalWaitingReason(reason string) bool {
	// CrashLoopBackOff is treated as terminal here because we set restartPolicy=Never,
	// so it should never legitimately appear; if it does, something is very wrong.
	switch reason {
	case "ImagePullBackOff", "ErrImagePull", "InvalidImageName", "CreateContainerConfigError",
		"CreateContainerError", "CrashLoopBackOff":
		return true
	}
	// Defensive: any Reason starting with "Err" is likely terminal.
	return strings.HasPrefix(reason, "Err")
}
