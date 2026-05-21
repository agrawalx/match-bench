package k8s

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/crane"
	"github.com/iicpc/build-worker/internal/precheck"
	"github.com/iicpc/schemas/topics"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	jobTTL       = int32(300)
	jobDeadline  = int64(1800)
	pollInterval = 5 * time.Second
)

type StatusUpdater interface {
	PublishStatus(ctx context.Context, submissionID, status, message string) error
	UpdateDBStatus(ctx context.Context, submissionID, status, message string) error
}

type MinioClient interface {
	DownloadObject(ctx context.Context, objectPath string) ([]byte, error)
}

type JobConfig struct {
	Namespace string // "build" — Jobs run in build namespace on Build Pool
	Image     string // all-in-one build-worker image

	DBUrl          string
	MinioEndpoint  string
	MinioAccessKey string
	MinioSecretKey string
	MinioBucket    string
	KafkaBrokers   string

	HarborStagingEndpoint    string // e.g. harbor-staging.example.com
	HarborProductionEndpoint string // e.g. harbor.example.com — if empty, promote is skipped
	HarborProject            string // e.g. iicpc
	HarborUser               string
	HarborPassword           string
}

type Spawner struct {
	client  kubernetes.Interface
	cfg     JobConfig
	minio   MinioClient
	updater StatusUpdater
	log     *slog.Logger
}

func NewSpawner(cfg JobConfig, minio MinioClient, updater StatusUpdater, log *slog.Logger) (*Spawner, error) {
	k8sCfg, err := loadK8sConfig()
	if err != nil {
		return nil, fmt.Errorf("k8s config: %w", err)
	}
	client, err := kubernetes.NewForConfig(k8sCfg)
	if err != nil {
		return nil, fmt.Errorf("k8s client: %w", err)
	}
	return &Spawner{client: client, cfg: cfg, minio: minio, updater: updater, log: log}, nil
}

func (s *Spawner) Run(ctx context.Context, msg topics.SubmissionBuildRequested) {
	id := msg.SubmissionID
	log := s.log.With("submission_id", id)

	// Pre-check: zip-slip scan before any job creation
	zipData, err := s.minio.DownloadObject(ctx, msg.ArtifactPath)
	if err != nil {
		log.Error("failed to download artifact for zip-slip check", "error", err)
		s.setStatus(ctx, id, topics.StatusFailed, fmt.Sprintf("download artifact: %v", err))
		return
	}
	if err := precheck.CheckZipSlip(zipData); err != nil {
		log.Warn("zip-slip check failed", "error", err)
		s.setStatus(ctx, id, topics.StatusFailed, fmt.Sprintf("zip-slip: %v", err))
		return
	}

	// Phase 1: Kaniko build → Harbor staging
	buildJob := "build-" + id[:8]
	if err := s.createJob(ctx, s.buildJobSpec(buildJob, msg, "build")); err != nil {
		log.Error("failed to create build job", "error", err)
		s.setStatus(ctx, id, topics.StatusFailed, fmt.Sprintf("create build job: %v", err))
		return
	}
	log.Info("build job created", "job", buildJob)

	if err := s.waitForJob(ctx, buildJob); err != nil {
		log.Error("build job failed", "error", err)
		s.setStatus(ctx, id, topics.StatusFailed, fmt.Sprintf("build: %v", err))
		return
	}
	s.setStatus(ctx, id, topics.StatusBuilding, "image built and pushed to staging registry")
	log.Info("phase 1 complete")

	// Phase 2: Trivy scan + Syft SBOM in parallel
	scanJob := "scan-" + id[:8]
	sbomJob := "sbom-" + id[:8]

	if err := s.createJob(ctx, s.buildJobSpec(scanJob, msg, "scan")); err != nil {
		s.setStatus(ctx, id, topics.StatusFailed, fmt.Sprintf("create scan job: %v", err))
		return
	}
	if err := s.createJob(ctx, s.buildJobSpec(sbomJob, msg, "sbom")); err != nil {
		s.setStatus(ctx, id, topics.StatusFailed, fmt.Sprintf("create sbom job: %v", err))
		return
	}
	log.Info("scan and sbom jobs created")

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	statuses := make(chan string, 2)

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := s.waitForJob(ctx, scanJob); err != nil {
			errs <- fmt.Errorf("scan: %w", err)
			return
		}
		statuses <- topics.StatusScanned
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := s.waitForJob(ctx, sbomJob); err != nil {
			errs <- fmt.Errorf("sbom: %w", err)
			return
		}
		statuses <- topics.StatusSBOMReady
	}()

	go func() {
		wg.Wait()
		close(statuses)
		close(errs)
	}()

	for st := range statuses {
		s.setStatus(ctx, id, st, st+" complete")
		log.Info("phase 2 step complete", "status", st)
	}
	for err := range errs {
		if err != nil {
			log.Error("phase 2 job failed", "error", err)
			s.setStatus(ctx, id, topics.StatusFailed, err.Error())
			return
		}
	}
	log.Info("phase 2 complete")

	// Phase 3: Promote staging → production
	if s.cfg.HarborProductionEndpoint != "" {
		stagingRef := fmt.Sprintf("%s/%s/%s:latest", s.cfg.HarborStagingEndpoint, s.cfg.HarborProject, id)
		productionRef := fmt.Sprintf("%s/%s/%s:latest", s.cfg.HarborProductionEndpoint, s.cfg.HarborProject, id)

		auth := crane.WithAuth(authn.FromConfig(authn.AuthConfig{
			Username: s.cfg.HarborUser,
			Password: s.cfg.HarborPassword,
		}))
		if err := crane.Copy(stagingRef, productionRef, auth, crane.WithContext(ctx)); err != nil {
			log.Error("harbor promote failed", "error", err)
			s.setStatus(ctx, id, topics.StatusFailed, fmt.Sprintf("promote staging→production: %v", err))
			return
		}
		log.Info("image promoted to production", "ref", productionRef)
	}

	s.setStatus(ctx, id, topics.StatusReady, "image ready")
	log.Info("pipeline complete")
}

func (s *Spawner) createJob(ctx context.Context, job *batchv1.Job) error {
	_, err := s.client.BatchV1().Jobs(s.cfg.Namespace).Create(ctx, job, metav1.CreateOptions{})
	return err
}

func (s *Spawner) waitForJob(ctx context.Context, jobName string) error {
	deadline := time.After(time.Duration(jobDeadline) * time.Second)
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline:
			return fmt.Errorf("job %s timed out after 30m", jobName)
		case <-ticker.C:
			job, err := s.client.BatchV1().Jobs(s.cfg.Namespace).Get(ctx, jobName, metav1.GetOptions{})
			if err != nil {
				s.log.Warn("job poll error", "job", jobName, "error", err)
				continue
			}
			if isJobFailed(job) {
				return fmt.Errorf("job %s failed: %s", jobName, jobFailureMessage(job))
			}
			if isJobComplete(job) {
				return nil
			}
		}
	}
}

func (s *Spawner) buildJobSpec(jobName string, msg topics.SubmissionBuildRequested, mode string) *batchv1.Job {
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
				"mode":          mode,
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
						"mode":          mode,
					},
				},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
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
							Env:     s.buildEnv(msg, mode),
						},
					},
				},
			},
		},
	}
}

func (s *Spawner) buildEnv(msg topics.SubmissionBuildRequested, mode string) []corev1.EnvVar {
	return []corev1.EnvVar{
		{Name: "RUNNER_MODE", Value: mode},
		{Name: "SUBMISSION_ID", Value: msg.SubmissionID},
		{Name: "ARTIFACT_PATH", Value: msg.ArtifactPath},
		{Name: "MINIO_ENDPOINT", Value: s.cfg.MinioEndpoint},
		{Name: "MINIO_ACCESS_KEY", Value: s.cfg.MinioAccessKey},
		{Name: "MINIO_SECRET_KEY", Value: s.cfg.MinioSecretKey},
		{Name: "MINIO_BUCKET", Value: s.cfg.MinioBucket},
		{Name: "HARBOR_STAGING_ENDPOINT", Value: s.cfg.HarborStagingEndpoint},
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
