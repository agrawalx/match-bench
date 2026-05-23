package k8s

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/crane"
	"github.com/iicpc/build-worker/internal/dockerfile"
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
	jobTTL        = int32(300)  // 5 min — generous; logs read within seconds of completion
	buildDeadline = int64(600)  // 10 min — Rust release builds need the headroom
	scanDeadline  = int64(900)  // 15 min — trivy cold DB download on ephemeral pods
	sbomDeadline  = int64(600)  // 10 min — syft is fine here
	pollInterval  = 5 * time.Second
)

type StatusUpdater interface {
	PublishStatus(ctx context.Context, submissionID, status, message string) error
	UpdateDBStatus(ctx context.Context, submissionID, status, message string) error
}

type MinioClient interface {
	DownloadObject(ctx context.Context, objectPath string) ([]byte, error)
	UploadBytes(ctx context.Context, objectPath, contentType string, data []byte) error
}

// JobConfig holds image refs and credentials for all three Job types.
// No runner-specific config (KAFKA_BROKERS, DATABASE_URL) — Jobs only interact
// with MinIO and Harbor; the spawner owns all status updates.
type JobConfig struct {
	Namespace    string
	SpawnerImage string // harbor.example.com/iicpc/spawner:latest — used as fetcher init container
	KanikoImage  string // gcr.io/kaniko-project/executor:v1.23
	TrivyImage   string // aquasec/trivy:0.51
	SyftImage    string // anchore/syft:v1.4

	MinioEndpoint  string
	MinioAccessKey string
	MinioSecretKey string
	MinioBucket    string

	HarborStagingEndpoint    string // e.g. harbor-staging.example.com
	HarborProductionEndpoint string // e.g. harbor.example.com — if empty, promote is skipped
	HarborProject            string // e.g. iicpc
	HarborUser               string
	HarborPassword           string

	// BuildNodePool, when non-empty, pins Job pods to nodes with pool=<value> label
	// and adds the build=true:NoSchedule toleration. Leave empty for single-node dev.
	BuildNodePool string
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
	stagingRef := fmt.Sprintf("%s/%s/%s:latest", s.cfg.HarborStagingEndpoint, s.cfg.HarborProject, id)

	// Pre-check: zip-slip scan before any Job is created
	zipData, err := s.minio.DownloadObject(ctx, msg.ArtifactPath)
	if err != nil {
		log.Error("failed to download artifact", "error", err)
		s.setStatus(ctx, id, topics.StatusFailed, fmt.Sprintf("download artifact: %v", err))
		return
	}
	if err := precheck.CheckZipSlip(zipData); err != nil {
		log.Warn("zip-slip check failed", "error", err)
		s.setStatus(ctx, id, topics.StatusFailed, fmt.Sprintf("zip-slip: %v", err))
		return
	}

	// Generate a platform-controlled Dockerfile from the submission metadata.
	// Contestants never supply a Dockerfile — the platform controls the build environment.
	dockerfileContent, err := dockerfile.Generate(msg.Language, msg.BuildType, msg.BuildTarget, msg.Port)
	if err != nil {
		log.Error("failed to generate dockerfile", "error", err)
		s.setStatus(ctx, id, topics.StatusFailed, fmt.Sprintf("generate dockerfile: %v", err))
		return
	}
	
	dockerfileB64 := base64.StdEncoding.EncodeToString([]byte(dockerfileContent))

	// Phase 1: fetcher init container extracts ZIP → kaniko builds and pushes to Harbor staging
	buildJobName := "build-" + id[:8]
	if err := s.createJob(ctx, s.buildJobSpec(buildJobName, msg, stagingRef, dockerfileB64)); err != nil {
		log.Error("failed to create build job", "error", err)
		s.setStatus(ctx, id, topics.StatusFailed, fmt.Sprintf("create build job: %v", err))
		return
	}
	log.Info("build job created", "job", buildJobName)

	buildErr := s.waitForJob(ctx, buildJobName, time.Duration(buildDeadline+60)*time.Second)
	buildLog, _ := s.readJobLogs(ctx, buildJobName, "build")
	_ = s.minio.UploadBytes(ctx, "submissions/"+id+"/build.log", "text/plain", buildLog)
	if buildErr != nil {
		log.Error("build job failed", "error", buildErr)
		s.setStatus(ctx, id, topics.StatusFailed, fmt.Sprintf("build: %v", buildErr))
		return
	}
	s.setStatus(ctx, id, topics.StatusBuilding, "image built and pushed to staging")
	log.Info("phase 1 complete")

	// Phase 2: Trivy scan + Syft SBOM run in parallel against the staging registry image
	scanJobName := "scan-" + id[:8]
	sbomJobName := "sbom-" + id[:8]

	if err := s.createJob(ctx, s.scanJobSpec(scanJobName, id, stagingRef)); err != nil {
		s.setStatus(ctx, id, topics.StatusFailed, fmt.Sprintf("create scan job: %v", err))
		return
	}
	if err := s.createJob(ctx, s.sbomJobSpec(sbomJobName, id, stagingRef)); err != nil {
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
		if err := s.waitForJob(ctx, scanJobName, time.Duration(scanDeadline+60)*time.Second); err != nil {
			errs <- fmt.Errorf("scan: %w", err)
			return
		}
		report, _ := s.readJobLogs(ctx, scanJobName, "")
		_ = s.minio.UploadBytes(ctx, "submissions/"+id+"/trivy-report.json", "application/json", report)
		statuses <- topics.StatusScanned
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := s.waitForJob(ctx, sbomJobName, time.Duration(sbomDeadline+60)*time.Second); err != nil {
			errs <- fmt.Errorf("sbom: %w", err)
			return
		}
		sbom, _ := s.readJobLogs(ctx, sbomJobName, "")
		_ = s.minio.UploadBytes(ctx, "submissions/"+id+"/sbom.json", "application/json", sbom)
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

	// Phase 3: promote staging → production via crane.Copy (no extra Job)
	if s.cfg.HarborProductionEndpoint != "" {
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

func (s *Spawner) waitForJob(ctx context.Context, jobName string, timeout time.Duration) error {
	deadline := time.After(timeout)
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline:
			return fmt.Errorf("job %s timed out after %s", jobName, timeout)
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

// readJobLogs reads stdout from the pod that ran jobName.
// container selects a specific container by name; pass "" for single-container Jobs.
func (s *Spawner) readJobLogs(ctx context.Context, jobName, container string) ([]byte, error) {
	pods, err := s.client.CoreV1().Pods(s.cfg.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: "job-name=" + jobName,
	})
	if err != nil {
		return nil, fmt.Errorf("list pods for job %s: %w", jobName, err)
	}
	if len(pods.Items) == 0 {
		return nil, fmt.Errorf("no pod found for job %s", jobName)
	}
	opts := &corev1.PodLogOptions{}
	if container != "" {
		opts.Container = container
	}
	req := s.client.CoreV1().Pods(s.cfg.Namespace).GetLogs(pods.Items[0].Name, opts)
	rc, err := req.Stream(ctx)
	if err != nil {
		return nil, fmt.Errorf("get logs for %s: %w", jobName, err)
	}
	defer rc.Close()
	return io.ReadAll(rc)
}

// buildJobSpec creates the kaniko build Job.
// Init container (fetcher): downloads ZIP from MinIO → extracts to /workspace, writes kaniko docker config.
// Main container (kaniko): reads /workspace, pushes built image to Harbor staging.
func (s *Spawner) buildJobSpec(jobName string, msg topics.SubmissionBuildRequested, stagingRef, dockerfileB64 string) *batchv1.Job {
	ttl := jobTTL
	deadline := buildDeadline
	backoff := int32(0)

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: s.cfg.Namespace,
			Labels: map[string]string{
				"app":           "build-worker",
				"submission-id": msg.SubmissionID,
				"mode":          "build",
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
						"mode":          "build",
					},
				},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Tolerations:   s.buildTolerations(),
					NodeSelector:  s.buildNodeSelector(),
					Volumes: []corev1.Volume{
						{
							Name:         "workspace",
							VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
							// we use emptyDir as its the temporary directory and jobs are cleaned up after completion 
						},
						{
							Name:         "kaniko-config",
							VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
						},
					},
					InitContainers: []corev1.Container{
						{
							Name:    "fetch",
							Image:   s.cfg.SpawnerImage,
							Command: []string{"/usr/local/bin/fetcher"},
							Env:     s.fetcherEnv(msg, dockerfileB64),
							VolumeMounts: []corev1.VolumeMount{
								{Name: "workspace", MountPath: "/workspace"},
								{Name: "kaniko-config", MountPath: "/kaniko/.docker"},
							},
						},
					},
					Containers: []corev1.Container{
						{
							Name:  "build",
							Image: s.cfg.KanikoImage,
							Args: []string{
								"--context=dir:///workspace",
								"--dockerfile=/workspace/Dockerfile",
								"--destination=" + stagingRef,
							},
							VolumeMounts: []corev1.VolumeMount{
								{Name: "workspace", MountPath: "/workspace"},
								{Name: "kaniko-config", MountPath: "/kaniko/.docker", ReadOnly: true},
							},
						},
					},
				},
			},
		},
	}
}

// scanJobSpec creates a trivy vulnerability scan Job against the staging registry image.
// JSON report is written to stdout and collected by the spawner via pod logs.
func (s *Spawner) scanJobSpec(jobName, submissionID, stagingRef string) *batchv1.Job {
	ttl := jobTTL
	deadline := scanDeadline
	backoff := int32(0)

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: s.cfg.Namespace,
			Labels:    map[string]string{"app": "build-worker", "submission-id": submissionID, "mode": "scan"},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:            &backoff,
			ActiveDeadlineSeconds:   &deadline,
			TTLSecondsAfterFinished: &ttl,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{"app": "build-job", "submission-id": submissionID, "mode": "scan"},
				},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Tolerations:   s.buildTolerations(),
					NodeSelector:  s.buildNodeSelector(),
					Containers: []corev1.Container{
						{
							Name:  "scan",
							Image: s.cfg.TrivyImage,
							Args: []string{
								"image",
								"--format", "json",
								"--quiet",
								"--username", s.cfg.HarborUser,
								"--password", s.cfg.HarborPassword,
								stagingRef,
							},
						},
					},
				},
			},
		},
	}
}

// sbomJobSpec creates a syft SBOM generation Job against the staging registry image.
// SPDX-JSON output is written to stdout and collected by the spawner via pod logs.
func (s *Spawner) sbomJobSpec(jobName, submissionID, stagingRef string) *batchv1.Job {
	ttl := jobTTL
	deadline := sbomDeadline
	backoff := int32(0)
	authority := strings.SplitN(stagingRef, "/", 2)[0]

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: s.cfg.Namespace,
			Labels:    map[string]string{"app": "build-worker", "submission-id": submissionID, "mode": "sbom"},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:            &backoff,
			ActiveDeadlineSeconds:   &deadline,
			TTLSecondsAfterFinished: &ttl,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{"app": "build-job", "submission-id": submissionID, "mode": "sbom"},
				},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Tolerations:   s.buildTolerations(),
					NodeSelector:  s.buildNodeSelector(),
					Containers: []corev1.Container{
						{
							Name:  "sbom",
							Image: s.cfg.SyftImage,
							Args: []string{
								"registry:" + stagingRef,
								"-o", "spdx-json",
								"-q",
							},
							Env: []corev1.EnvVar{
								{Name: "SYFT_REGISTRY_AUTH_AUTHORITY", Value: authority},
								{Name: "SYFT_REGISTRY_AUTH_USERNAME", Value: s.cfg.HarborUser},
								{Name: "SYFT_REGISTRY_AUTH_PASSWORD", Value: s.cfg.HarborPassword},
							},
						},
					},
				},
			},
		},
	}
}

func (s *Spawner) fetcherEnv(msg topics.SubmissionBuildRequested, dockerfileB64 string) []corev1.EnvVar {
	return []corev1.EnvVar{
		{Name: "MINIO_ENDPOINT", Value: s.cfg.MinioEndpoint},
		{Name: "MINIO_ACCESS_KEY", Value: s.cfg.MinioAccessKey},
		{Name: "MINIO_SECRET_KEY", Value: s.cfg.MinioSecretKey},
		{Name: "MINIO_BUCKET", Value: s.cfg.MinioBucket},
		{Name: "ARTIFACT_PATH", Value: msg.ArtifactPath},
		{Name: "HARBOR_STAGING_ENDPOINT", Value: s.cfg.HarborStagingEndpoint},
		{Name: "HARBOR_USER", Value: s.cfg.HarborUser},
		{Name: "HARBOR_PASSWORD", Value: s.cfg.HarborPassword},
		{Name: "DOCKERFILE_B64", Value: dockerfileB64},
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

func (s *Spawner) buildTolerations() []corev1.Toleration {
	if s.cfg.BuildNodePool == "" {
		return nil
	}
	return []corev1.Toleration{{
		Key:      "build",
		Operator: corev1.TolerationOpEqual,
		Value:    "true",
		Effect:   corev1.TaintEffectNoSchedule,
	}}
}

func (s *Spawner) buildNodeSelector() map[string]string {
	if s.cfg.BuildNodePool == "" {
		return nil
	}
	return map[string]string{"pool": s.cfg.BuildNodePool}
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
	// the next two lines only runs on local_dev, on real clusters we always have inClusterConfig set automatically 
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, nil).ClientConfig()
}
