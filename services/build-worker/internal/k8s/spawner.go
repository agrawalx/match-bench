package k8s

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/iicpc/schemas/topics"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	jobTTL       = int32(300)  // seconds before completed Job is auto-deleted by k8s
	jobDeadline  = int64(1800) // 30 min hard cap per build
	pollInterval = 5 * time.Second
)

type StatusUpdater interface {
	PublishStatus(ctx context.Context, submissionID, status, message string) error
	UpdateDBStatus(ctx context.Context, submissionID, status, message string) error
}

type JobConfig struct {
	// k8s
	Namespace string // "build" — Jobs run in build namespace on Build Pool
	Image     string // all-in-one build-worker image

	// credentials passed as env vars into the Job container
	DBUrl          string
	MinioEndpoint  string
	MinioAccessKey string
	MinioSecretKey string
	MinioBucket    string
	KafkaBrokers   string

	// Harbor — optional; push is skipped if empty
	HarborEndpoint string // e.g. harbor.example.com
	HarborProject  string // e.g. iicpc
	HarborUser     string
	HarborPassword string
}

type Spawner struct {
	client  kubernetes.Interface
	cfg     JobConfig
	updater StatusUpdater
	log     *slog.Logger
}

func NewSpawner(cfg JobConfig, updater StatusUpdater, log *slog.Logger) (*Spawner, error) {
	k8sCfg, err := loadK8sConfig()
	if err != nil {
		return nil, fmt.Errorf("k8s config: %w", err)
	}
	client, err := kubernetes.NewForConfig(k8sCfg)
	if err != nil {
		return nil, fmt.Errorf("k8s client: %w", err)
	}
	return &Spawner{client: client, cfg: cfg, updater: updater, log: log}, nil
}

func (s *Spawner) Run(ctx context.Context, msg topics.SubmissionBuildRequested) {
	jobName := "build-" + msg.SubmissionID[:8]
	log := s.log.With("submission_id", msg.SubmissionID, "job", jobName)

	s.setStatus(ctx, msg.SubmissionID, topics.StatusBuilding, "job created")

	job := s.buildJobSpec(jobName, msg)
	if _, err := s.client.BatchV1().Jobs(s.cfg.Namespace).Create(ctx, job, metav1.CreateOptions{}); err != nil {
		log.Error("failed to create job", "error", err)
		s.setStatus(ctx, msg.SubmissionID, topics.StatusFailed, fmt.Sprintf("job create: %v", err))
		return
	}
	log.Info("job created", "namespace", s.cfg.Namespace)

	// Runner container publishes intermediate statuses (scanned, sbom_ready, ready).
	// Spawner only handles failure and timeout.
	go s.watchJob(ctx, jobName, msg.SubmissionID)
}

func (s *Spawner) watchJob(ctx context.Context, jobName, submissionID string) {
	deadline := time.After(time.Duration(jobDeadline) * time.Second)
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-deadline:
			s.setStatus(ctx, submissionID, topics.StatusFailed, "build timed out after 30 minutes")
			return
		case <-ticker.C:
			job, err := s.client.BatchV1().Jobs(s.cfg.Namespace).Get(ctx, jobName, metav1.GetOptions{})
			if err != nil {
				s.log.Warn("job poll error", "job", jobName, "error", err)
				continue
			}
			if isJobFailed(job) {
				s.setStatus(ctx, submissionID, topics.StatusFailed, jobFailureMessage(job))
				return
			}
			if isJobComplete(job) {
				return
			}
		}
	}
}

func (s *Spawner) buildJobSpec(jobName string, msg topics.SubmissionBuildRequested) *batchv1.Job {
	ttl := jobTTL
	deadline := jobDeadline
	backoff := int32(0)

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: s.cfg.Namespace,
			Labels: map[string]string{
				"app":           "build-worker",
				"submission-id": msg.SubmissionID,
			},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:            &backoff,
			ActiveDeadlineSeconds:   &deadline,
			TTLSecondsAfterFinished: &ttl,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app":           "build-job",
						"submission-id": msg.SubmissionID,
					},
				},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					// Schedule only on the Build Pool (taint: build=true:NoSchedule)
					Tolerations: []corev1.Toleration{
						{
							Key:      "build",
							Operator: corev1.TolerationOpEqual,
							Value:    "true",
							Effect:   corev1.TaintEffectNoSchedule,
						},
					},
					NodeSelector: map[string]string{
						"pool": "build",
					},
					Containers: []corev1.Container{
						{
							Name:    "runner",
							Image:   s.cfg.Image,
							Command: []string{"/usr/local/bin/runner"},
							Env:     s.buildEnv(msg),
						},
					},
				},
			},
		},
	}
}

func (s *Spawner) buildEnv(msg topics.SubmissionBuildRequested) []corev1.EnvVar {
	return []corev1.EnvVar{
		{Name: "SUBMISSION_ID", Value: msg.SubmissionID},
		{Name: "ARTIFACT_PATH", Value: msg.ArtifactPath},
		{Name: "DATABASE_URL", Value: s.cfg.DBUrl},
		{Name: "MINIO_ENDPOINT", Value: s.cfg.MinioEndpoint},
		{Name: "MINIO_ACCESS_KEY", Value: s.cfg.MinioAccessKey},
		{Name: "MINIO_SECRET_KEY", Value: s.cfg.MinioSecretKey},
		{Name: "MINIO_BUCKET", Value: s.cfg.MinioBucket},
		{Name: "KAFKA_BROKERS", Value: s.cfg.KafkaBrokers},
		{Name: "HARBOR_ENDPOINT", Value: s.cfg.HarborEndpoint},
		{Name: "HARBOR_PROJECT", Value: s.cfg.HarborProject},
		{Name: "HARBOR_USER", Value: s.cfg.HarborUser},
		{Name: "HARBOR_PASSWORD", Value: s.cfg.HarborPassword},
	}
}

func (s *Spawner) setStatus(ctx context.Context, submissionID, status, message string) {
	if err := s.updater.PublishStatus(ctx, submissionID, status, message); err != nil {
		s.log.Warn("publish status failed", "error", err)
	}
	if err := s.updater.UpdateDBStatus(ctx, submissionID, status, message); err != nil {
		s.log.Warn("db status update failed", "error", err)
	}
}

func isJobComplete(job *batchv1.Job) bool {
	for _, c := range job.Status.Conditions {
		if c.Type == batchv1.JobComplete && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func isJobFailed(job *batchv1.Job) bool {
	for _, c := range job.Status.Conditions {
		if c.Type == batchv1.JobFailed && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func jobFailureMessage(job *batchv1.Job) string {
	for _, c := range job.Status.Conditions {
		if c.Type == batchv1.JobFailed {
			return c.Message
		}
	}
	return "job failed"
}

func loadK8sConfig() (*rest.Config, error) {
	if cfg, err := rest.InClusterConfig(); err == nil {
		return cfg, nil
	}
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, nil).ClientConfig()
}
