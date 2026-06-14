// Package k8s implements slot behavior.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package k8s

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"

	cerrs "github.com/iicpc/sandbox-orchestrator/internal/errors"
	"github.com/iicpc/sandbox-orchestrator/internal/store"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
)

const (
	LabelApp       = "app"
	LabelSlot      = "slot"
	LabelManagedBy = "app.kubernetes.io/managed-by"

	AppValue       = "algo"
	ManagedByValue = "sandbox-orchestrator"

	CaptureAppValue             = "ebpf-capture"
	captureContestantAnnotation = "iicpc.dev/contestant-id"
	captureObjectPath           = "/opt/iicpc/ebpf/libiicpc_ebpf_latency.so"

	slotActiveDeadlineSeconds       = int64(3600)
	captureJobActiveDeadlineSeconds = int64(3600)
)

var capturablePorts = map[int]struct{}{8080: {}, 9898: {}}

// captureJobName performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func captureJobName(slotID string) string { return "capture-" + slotID }

// Manager groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type Manager struct {
	client       kubernetes.Interface
	namespace    string
	runtimeClass string // empty → no runtimeClassName set; matches BUILD_NODE_POOL toggle pattern
	cpu          string // integer CPU count as a string, e.g. "2". Same value used for request AND limit (Guaranteed QoS).
	memory       string // memory size with unit, e.g. "1Gi". Same value used for request AND limit.
	nodePool     string // empty → no nodeAffinity/toleration; non-empty → pin to that node pool
	egressBwBps  string // empty → no annotation; non-empty → CNI bandwidth plugin throttles egress (e.g. "100M")
	ingressBwBps string // same for ingress

	captureEnabled      bool
	captureImage        string // image with the baked BPF object + userspace binary
	kafkaBrokers        string // passed to the capture so it can publish orders.acked
	imagePullSecretName string // optional registry credentials for contestant images
}

// Config groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type Config struct {
	Namespace        string
	RuntimeClass     string // e.g. "gvisor" in prod; empty in dev k3s
	CPU              string
	Memory           string
	NodePool         string
	EgressBandwidth  string
	IngressBandwidth string

	CaptureEnabled bool
	CaptureImage   string
	KafkaBrokers   string

	ImagePullSecretName string
}

// Namespace applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (m *Manager) Namespace() string { return m.namespace }

// NewManager performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func NewManager(client kubernetes.Interface, cfg Config) (*Manager, error) {
	if err := validateConfig(cfg); err != nil {
		return nil, err
	}
	return &Manager{
		client:              client,
		namespace:           cfg.Namespace,
		runtimeClass:        cfg.RuntimeClass,
		cpu:                 cfg.CPU,
		memory:              cfg.Memory,
		nodePool:            cfg.NodePool,
		egressBwBps:         cfg.EgressBandwidth,
		ingressBwBps:        cfg.IngressBandwidth,
		captureEnabled:      cfg.CaptureEnabled,
		captureImage:        cfg.CaptureImage,
		kafkaBrokers:        cfg.KafkaBrokers,
		imagePullSecretName: cfg.ImagePullSecretName,
	}, nil
}

// validateConfig performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func validateConfig(cfg Config) error {
	if cfg.Namespace == "" {
		return fmt.Errorf("namespace is required")
	}
	if cfg.CPU != "" {
		cpu, err := resource.ParseQuantity(cfg.CPU)
		if err != nil {
			return fmt.Errorf("invalid CPU resource %q: %w", cfg.CPU, err)
		}
		if _, err := strconv.Atoi(cfg.CPU); err != nil || cpu.MilliValue()%1000 != 0 {
			return fmt.Errorf("CPU must be an integer core count for cpuset pinning, got %q", cfg.CPU)
		}
	}
	if cfg.Memory != "" {
		if _, err := resource.ParseQuantity(cfg.Memory); err != nil {
			return fmt.Errorf("invalid memory resource %q: %w", cfg.Memory, err)
		}
	}
	if cfg.CaptureEnabled && cfg.CaptureImage == "" {
		return fmt.Errorf("CAPTURE_IMAGE is required when capture is enabled")
	}
	return nil
}

// CreateSlot applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (m *Manager) CreateSlot(ctx context.Context, slotID, contestantID, image string, port int) error {
	if m.captureEnabled {
		if _, ok := capturablePorts[port]; !ok {
			return fmt.Errorf("%w: port %d is not in the eBPF-capture set {8080, 9898}", cerrs.ErrInvalidRequest, port)
		}
	}

	resourceName := podName(slotID)

	existing, err := m.ensurePod(ctx, resourceName, slotID, contestantID, image, port)
	if err != nil {
		return err
	}
	if len(existing.Spec.Containers) == 0 || existing.Spec.Containers[0].Image != image {
		existingImage := ""
		if len(existing.Spec.Containers) > 0 {
			existingImage = existing.Spec.Containers[0].Image
		}
		return fmt.Errorf("%w: existing pod image %q", cerrs.ErrSlotImageMismatch, existingImage)
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

// ensurePod applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (m *Manager) ensurePod(ctx context.Context, resourceName, slotID, contestantID, image string, port int) (*corev1.Pod, error) {
	existing, err := m.client.CoreV1().Pods(m.namespace).Get(ctx, resourceName, metav1.GetOptions{})
	switch {
	case err == nil:
		return existing, nil
	case apierrors.IsNotFound(err):
		created, err := m.client.CoreV1().Pods(m.namespace).Create(ctx, m.podSpec(slotID, contestantID, image, port), metav1.CreateOptions{})
		if err != nil {
			if apierrors.IsAlreadyExists(err) {
				existing, err := m.client.CoreV1().Pods(m.namespace).Get(ctx, resourceName, metav1.GetOptions{})
				if err != nil {
					return nil, fmt.Errorf("get pod after create race: %w", err)
				}
				return existing, nil
			}
			return nil, fmt.Errorf("create pod: %w", err)
		}
		return created, nil
	default:
		return nil, fmt.Errorf("get pod: %w", err)
	}
}

// DeleteSlot applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (m *Manager) DeleteSlot(ctx context.Context, slotID string) error {
	resourceName := podName(slotID)

	if err := m.client.CoreV1().Pods(m.namespace).Delete(ctx, resourceName, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete pod: %w", err)
	}
	if err := m.client.CoreV1().Services(m.namespace).Delete(ctx, resourceName, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete service: %w", err)
	}
	if m.captureEnabled {
		policy := metav1.DeletePropagationBackground
		if err := m.client.BatchV1().Jobs(m.namespace).Delete(ctx, captureJobName(slotID), metav1.DeleteOptions{PropagationPolicy: &policy}); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("delete capture job: %w", err)
		}
	}
	return nil
}

// Refresh applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (m *Manager) Refresh(ctx context.Context, slotID string) (store.SlotState, string, error) {
	pod, err := m.client.CoreV1().Pods(m.namespace).Get(ctx, podName(slotID), metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return "", "", cerrs.ErrSlotNotFound
		}
		return "", "", fmt.Errorf("get pod: %w", err)
	}
	state, msg := deriveState(pod)
	if state == store.StateReady && m.captureEnabled {
		if err := m.ensureCapture(ctx, pod); err != nil {
			slog.Default().Warn("ensure capture job", "slot_id", slotID, "error", err)
		}
	}
	return state, msg, nil
}

// ListExisting applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (m *Manager) ListExisting(ctx context.Context) ([]store.Slot, int, error) {
	selector := fmt.Sprintf("%s=%s,%s=%s", LabelApp, AppValue, LabelManagedBy, ManagedByValue)
	pods, err := m.client.CoreV1().Pods(m.namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return nil, 0, fmt.Errorf("list existing pods: %w", err)
	}

	out := make([]store.Slot, 0, len(pods.Items))
	skippedMissingSlotLabel := 0
	for _, pod := range pods.Items {
		slotID := pod.Labels[LabelSlot]
		if slotID == "" {
			skippedMissingSlotLabel++
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

	if m.captureEnabled {
		live := make(map[string]struct{}, len(out))
		for _, s := range out {
			live[s.SlotID] = struct{}{}
		}
		m.reapOrphanCaptureJobs(ctx, live)
	}
	return out, skippedMissingSlotLabel, nil
}

// reapOrphanCaptureJobs applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (m *Manager) reapOrphanCaptureJobs(ctx context.Context, liveSlots map[string]struct{}) {
	selector := fmt.Sprintf("%s=%s,%s=%s", LabelApp, CaptureAppValue, LabelManagedBy, ManagedByValue)
	jobs, err := m.client.BatchV1().Jobs(m.namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		slog.Default().Warn("list capture jobs for reaping", "error", err)
		return
	}
	policy := metav1.DeletePropagationBackground
	for i := range jobs.Items {
		slotID := jobs.Items[i].Labels[LabelSlot]
		if _, ok := liveSlots[slotID]; ok {
			continue
		}
		if err := m.client.BatchV1().Jobs(m.namespace).Delete(ctx, jobs.Items[i].Name, metav1.DeleteOptions{PropagationPolicy: &policy}); err != nil && !apierrors.IsNotFound(err) {
			slog.Default().Warn("reap orphan capture job", "job", jobs.Items[i].Name, "error", err)
		}
	}
}

// podName performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func podName(slotID string) string {
	return "algo-" + slotID
}

// ServiceFQDN performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func ServiceFQDN(slotID, namespace string) string {
	return fmt.Sprintf("%s.%s.svc.cluster.local", podName(slotID), namespace)
}

// podSpec applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (m *Manager) podSpec(slotID, contestantID, image string, port int) *corev1.Pod {
	labels := map[string]string{
		LabelApp:       AppValue,
		LabelSlot:      slotID,
		LabelManagedBy: ManagedByValue,
	}

	annotations := map[string]string{}
	if m.egressBwBps != "" {
		annotations["kubernetes.io/egress-bandwidth"] = m.egressBwBps
	}
	if m.ingressBwBps != "" {
		annotations["kubernetes.io/ingress-bandwidth"] = m.ingressBwBps
	}
	if contestantID != "" {
		annotations[captureContestantAnnotation] = contestantID
	}

	autoMount := false
	readOnlyRoot := true
	noPrivEsc := false
	deadline := slotActiveDeadlineSeconds

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        podName(slotID),
			Namespace:   m.namespace,
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: corev1.PodSpec{
			RestartPolicy:                corev1.RestartPolicyNever,
			ActiveDeadlineSeconds:        &deadline,
			AutomountServiceAccountToken: &autoMount,
			Volumes:                      writableVolumes(),
			Containers: []corev1.Container{{
				Name:            "algo",
				Image:           image,
				ImagePullPolicy: corev1.PullIfNotPresent,
				Ports:           []corev1.ContainerPort{{ContainerPort: int32(port), Protocol: corev1.ProtocolTCP}},
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
					ReadOnlyRootFilesystem:   &readOnlyRoot,
					AllowPrivilegeEscalation: &noPrivEsc,
					Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
					SeccompProfile:           &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
				},
				VolumeMounts: writableMounts(),
			}},
		},
	}

	if m.imagePullSecretName != "" {
		pod.Spec.ImagePullSecrets = []corev1.LocalObjectReference{{
			Name: m.imagePullSecretName,
		}}
	}

	if m.runtimeClass != "" {
		rc := m.runtimeClass
		pod.Spec.RuntimeClassName = &rc
	}

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

// writableVolumes performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
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

// writableMounts performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func writableMounts() []corev1.VolumeMount {
	return []corev1.VolumeMount{
		{Name: "tmp", MountPath: "/tmp"},
		{Name: "var-tmp", MountPath: "/var/tmp"},
		{Name: "var-log", MountPath: "/var/log"},
		{Name: "var-run", MountPath: "/var/run"},
	}
}

// serviceSpec applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
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

// ensureCapture applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (m *Manager) ensureCapture(ctx context.Context, pod *corev1.Pod) error {
	slotID := pod.Labels[LabelSlot]
	if slotID == "" {
		return nil
	}
	name := captureJobName(slotID)
	if _, err := m.client.BatchV1().Jobs(m.namespace).Get(ctx, name, metav1.GetOptions{}); err == nil {
		return nil // already created
	} else if !apierrors.IsNotFound(err) {
		return fmt.Errorf("get capture job: %w", err)
	}

	nodeName := pod.Spec.NodeName
	if nodeName == "" {
		return fmt.Errorf("algo pod not yet scheduled to a node")
	}
	containerID := ""
	if len(pod.Status.ContainerStatuses) > 0 {
		containerID = pod.Status.ContainerStatuses[0].ContainerID
	}
	job := m.captureJobSpec(slotID, pod.Annotations[captureContestantAnnotation], nodeName, string(pod.UID), containerID)
	if _, err := m.client.BatchV1().Jobs(m.namespace).Create(ctx, job, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create capture job: %w", err)
	}
	return nil
}

// captureJobSpec applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (m *Manager) captureJobSpec(slotID, contestantID, nodeName, podUID, containerID string) *batchv1.Job {
	labels := map[string]string{
		LabelApp:       CaptureAppValue,
		LabelSlot:      slotID,
		LabelManagedBy: ManagedByValue,
	}
	privileged := true
	autoMount := false
	backoffLimit := int32(0) // no retries; a failed attach is reported, not looped
	ttl := int32(300)        // self-clean finished Jobs after 5 min
	graceful := int64(5)
	captureDeadline := captureJobActiveDeadlineSeconds
	bpffsType := corev1.HostPathDirectoryOrCreate

	var tolerations []corev1.Toleration
	if m.nodePool != "" {
		tolerations = []corev1.Toleration{{
			Key:      "sandbox",
			Operator: corev1.TolerationOpEqual,
			Value:    "true",
			Effect:   corev1.TaintEffectNoSchedule,
		}}
	}

	env := []corev1.EnvVar{
		{Name: "RUST_LOG", Value: "info"},
		{Name: "SESSION_ID", Value: slotID},
		{Name: "CONTESTANT_ID", Value: contestantID},
		{Name: "EBPF_IFACE", Value: "eth0"},
		{Name: "EBPF_ALGO_POD_UID", Value: podUID},
		{Name: "EBPF_ALGO_CONTAINER_ID", Value: containerID},
		{Name: "EBPF_OBJECT_PATH", Value: captureObjectPath},
		{Name: "KAFKA_BROKERS", Value: m.kafkaBrokers},
	}

	ownerRefs := []metav1.OwnerReference{{
		APIVersion: "v1",
		Kind:       "Pod",
		Name:       podName(slotID),
		UID:        types.UID(podUID),
	}}

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:            captureJobName(slotID),
			Namespace:       m.namespace,
			Labels:          labels,
			OwnerReferences: ownerRefs, // GC the capture Job when the algo pod is deleted
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:            &backoffLimit,
			ActiveDeadlineSeconds:   &captureDeadline,
			TTLSecondsAfterFinished: &ttl,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
					Annotations: map[string]string{
						"prometheus.io/scrape": "true",
						"prometheus.io/path":   "/metrics",
						"prometheus.io/port":   "9090",
					},
				},
				Spec: corev1.PodSpec{
					RestartPolicy:                 corev1.RestartPolicyNever,
					NodeName:                      nodeName,
					HostPID:                       true,
					AutomountServiceAccountToken:  &autoMount,
					TerminationGracePeriodSeconds: &graceful,
					Tolerations:                   tolerations,
					Volumes: []corev1.Volume{{
						Name: "bpffs",
						VolumeSource: corev1.VolumeSource{
							HostPath: &corev1.HostPathVolumeSource{Path: "/sys/fs/bpf", Type: &bpffsType},
						},
					}},
					Containers: []corev1.Container{{
						Name:            "capture",
						Image:           m.captureImage,
						ImagePullPolicy: corev1.PullIfNotPresent,
						Env:             env,
						SecurityContext: &corev1.SecurityContext{
							Privileged: &privileged,
							Capabilities: &corev1.Capabilities{
								Add: []corev1.Capability{"BPF", "NET_ADMIN", "SYS_ADMIN"},
							},
						},
						Resources:    captureResources(),
						VolumeMounts: []corev1.VolumeMount{{Name: "bpffs", MountPath: "/sys/fs/bpf"}},
					}},
				},
			},
		},
	}
}

// captureResources performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func captureResources() corev1.ResourceRequirements {
	return corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("200m"),
			corev1.ResourceMemory: resource.MustParse("256Mi"),
		},
		Limits: corev1.ResourceList{
			// 4 (was 2): the userspace drain+parse+publish wants ~3 cores at >150k
			// delivered; a 2-core cap CFS-throttled it -> ringbuf drops. Burstable
			// (request stays 200m), so this is safe on a 4-vCPU node and lets the
			// capture use its needed cores on the c6i.2xlarge (8 vCPU) sandbox node.
			corev1.ResourceCPU:    resource.MustParse("4"),
			corev1.ResourceMemory: resource.MustParse("512Mi"),
		},
	}
}

// containerResources applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (m *Manager) containerResources() corev1.ResourceRequirements {
	list := corev1.ResourceList{}
	if m.cpu != "" {
		list[corev1.ResourceCPU] = resource.MustParse(m.cpu)
	}
	if m.memory != "" {
		list[corev1.ResourceMemory] = resource.MustParse(m.memory)
	}
	return corev1.ResourceRequirements{Requests: list.DeepCopy(), Limits: list.DeepCopy()}
}

// deriveState performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func deriveState(pod *corev1.Pod) (store.SlotState, string) {
	if pod.DeletionTimestamp != nil {
		return store.StateTerminating, "pod deletion in progress"
	}

	switch pod.Status.Phase {
	case corev1.PodFailed:
		return store.StateFailed, fmt.Sprintf("pod failed: %s", pod.Status.Message)
	case corev1.PodSucceeded:
		return store.StateFailed, "container exited unexpectedly (algo should stay running)"
	}

	for _, cs := range pod.Status.ContainerStatuses {
		if cs.State.Waiting == nil {
			continue
		}
		reason := cs.State.Waiting.Reason
		if isTerminalWaitingReason(reason) {
			return store.StateFailed, fmt.Sprintf("container %s: %s — %s", cs.Name, reason, cs.State.Waiting.Message)
		}
	}

	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
			return store.StateReady, "ready"
		}
	}

	return store.StateCreating, "pod scheduling / readiness probe pending"
}

// isTerminalWaitingReason performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func isTerminalWaitingReason(reason string) bool {
	switch reason {
	case "ImagePullBackOff", "ErrImagePull", "InvalidImageName", "CreateContainerConfigError",
		"CreateContainerError", "CrashLoopBackOff":
		return true
	}
	return strings.HasPrefix(reason, "Err")
}
