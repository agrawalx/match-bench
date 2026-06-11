// Package k8s implements spawner behavior.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package k8s

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
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
	"github.com/iicpc/libs/metrics"
	"github.com/iicpc/schemas/topics"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	jobTTL        = int32(300) // 5 min — generous; logs read within seconds of completion
	buildDeadline = int64(600) // 10 min — Rust release builds need the headroom
	scanDeadline  = int64(900) // 15 min — trivy cold DB download on ephemeral pods
	sbomDeadline  = int64(600) // 10 min — syft is fine here
	pollInterval  = 5 * time.Second
	maxK8sNameLen = 63
)

// StatusUpdater defines the behavior expected by this package boundary.
// Implementations should preserve the caller-visible contract.
type StatusUpdater interface {
	PublishStatus(ctx context.Context, submissionID, status, message string) error
	UpdateDBStatus(ctx context.Context, submissionID, status, message string) error
	UpdateImageRef(ctx context.Context, submissionID, imageRef string) error
}

// MinioClient defines the behavior expected by this package boundary.
// Implementations should preserve the caller-visible contract.
type MinioClient interface {
	DownloadObject(ctx context.Context, objectPath string) ([]byte, error)
	UploadBytes(ctx context.Context, objectPath, contentType string, data []byte) error
}

// JobConfig groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
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
	JobSecretName  string

	HarborStagingEndpoint    string // e.g. harbor-staging.example.com
	HarborProductionEndpoint string // e.g. harbor.example.com
	HarborProject            string // e.g. iicpc
	HarborUser               string
	HarborPassword           string

	RegistryProvider string

	RegistryInsecure bool

	BuildNodePool string
}

// ECRRepositoryClient defines the behavior expected by this package boundary.
// Implementations should preserve the caller-visible contract.
type ECRRepositoryClient interface {
	CreateRepository(ctx context.Context, repositoryName string) error
}

var newECRClient = newAWSECRClient

// Spawner groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type Spawner struct {
	client  kubernetes.Interface
	cfg     JobConfig
	minio   MinioClient
	updater StatusUpdater
	ecr     ECRRepositoryClient // nil unless cfg.RegistryProvider == "ecr"
	log     *slog.Logger
}

// NewSpawner performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func NewSpawner(cfg JobConfig, minio MinioClient, updater StatusUpdater, log *slog.Logger) (*Spawner, error) {
	if cfg.HarborProductionEndpoint == "" {
		return nil, fmt.Errorf("harbor production endpoint is required")
	}
	if cfg.JobSecretName == "" {
		return nil, fmt.Errorf("job secret name is required")
	}
	var ecrClient ECRRepositoryClient
	switch cfg.RegistryProvider {
	case "":
	case "ecr":
		c, err := newECRClient(context.Background())
		if err != nil {
			return nil, fmt.Errorf("ecr client: %w", err)
		}
		ecrClient = c
	default:
		return nil, fmt.Errorf("unknown REGISTRY_PROVIDER %q (supported: \"\" or \"ecr\")", cfg.RegistryProvider)
	}
	k8sCfg, err := loadK8sConfig()
	if err != nil {
		return nil, fmt.Errorf("k8s config: %w", err)
	}
	client, err := kubernetes.NewForConfig(k8sCfg)
	if err != nil {
		return nil, fmt.Errorf("k8s client: %w", err)
	}
	return &Spawner{client: client, cfg: cfg, minio: minio, updater: updater, ecr: ecrClient, log: log}, nil
}

// Run applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (s *Spawner) Run(ctx context.Context, msg topics.SubmissionBuildRequested) {
	runStart := time.Now()
	id := msg.SubmissionID
	log := s.log.With("submission_id", id)
	stagingRef := fmt.Sprintf("%s/%s/%s:latest", s.cfg.HarborStagingEndpoint, s.cfg.HarborProject, id)

	phaseStart := time.Now()
	zipData, err := s.minio.DownloadObject(ctx, msg.ArtifactPath)
	if err != nil {
		recordBuildPhase("precheck", phaseStart, "error")
		recordBuildRequest("error")
		log.Error("failed to download artifact", "error", err)
		s.setStatus(ctx, id, topics.StatusFailed, fmt.Sprintf("download artifact: %v", err))
		return
	}
	if err := precheck.CheckZipSlip(zipData); err != nil {
		recordBuildPhase("precheck", phaseStart, "error")
		recordBuildRequest("error")
		log.Warn("zip-slip check failed", "error", err)
		s.setStatus(ctx, id, topics.StatusFailed, fmt.Sprintf("zip-slip: %v", err))
		return
	}
	recordBuildPhase("precheck", phaseStart, "ok")

	dockerfileContent, err := dockerfile.Generate(msg.Language, msg.BuildType, msg.BuildTarget, msg.Port)
	if err != nil {
		recordBuildRequest("error")
		log.Error("failed to generate dockerfile", "error", err)
		s.setStatus(ctx, id, topics.StatusFailed, fmt.Sprintf("generate dockerfile: %v", err))
		return
	}

	dockerfileB64 := base64.StdEncoding.EncodeToString([]byte(dockerfileContent))

	buildJobName := resourceName("build", id)
	phaseStart = time.Now()
	s.setStatus(ctx, id, topics.StatusBuilding, "building image")
	if err := s.ensureRepository(ctx, stagingRef); err != nil {
		recordBuildPhase("build", phaseStart, "error")
		recordBuildRequest("error")
		log.Error("ensure staging repository failed", "error", err)
		s.setStatus(ctx, id, topics.StatusFailed, fmt.Sprintf("ensure staging repository: %v", err))
		return
	}
	if err := s.createJob(ctx, s.buildJobSpec(buildJobName, msg, stagingRef, dockerfileB64)); err != nil {
		recordBuildPhase("build", phaseStart, "error")
		recordBuildRequest("error")
		log.Error("failed to create build job", "error", err)
		s.setStatus(ctx, id, topics.StatusFailed, fmt.Sprintf("create build job: %v", err))
		return
	}
	metrics.Counter("build_jobs_created_total", "Build jobs created by mode.", metrics.Labels("mode", "build"), 1)
	log.Info("build job created", "job", buildJobName)

	buildErr := s.waitForJob(ctx, buildJobName, time.Duration(buildDeadline+60)*time.Second)
	buildLog, _ := s.readJobLogs(ctx, buildJobName, "build")
	_ = s.minio.UploadBytes(ctx, "submissions/"+id+"/build.log", "text/plain", buildLog)
	if buildErr != nil {
		recordBuildPhase("build", phaseStart, "error")
		metrics.Counter("build_jobs_failed_total", "Build jobs failed by mode.", metrics.Labels("mode", "build"), 1)
		recordBuildRequest("error")
		log.Error("build job failed", "error", buildErr)
		s.setStatus(ctx, id, topics.StatusFailed, fmt.Sprintf("build: %v", buildErr))
		return
	}
	recordBuildPhase("build", phaseStart, "ok")
	log.Info("phase 1 complete")

	scanJobName := resourceName("scan", id)
	sbomJobName := resourceName("sbom", id)

	if err := s.createJob(ctx, s.scanJobSpec(scanJobName, id, stagingRef)); err != nil {
		recordBuildRequest("error")
		s.setStatus(ctx, id, topics.StatusFailed, fmt.Sprintf("create scan job: %v", err))
		return
	}
	metrics.Counter("build_jobs_created_total", "Build jobs created by mode.", metrics.Labels("mode", "scan"), 1)
	if err := s.createJob(ctx, s.sbomJobSpec(sbomJobName, id, stagingRef)); err != nil {
		recordBuildRequest("error")
		s.setStatus(ctx, id, topics.StatusFailed, fmt.Sprintf("create sbom job: %v", err))
		return
	}
	metrics.Counter("build_jobs_created_total", "Build jobs created by mode.", metrics.Labels("mode", "sbom"), 1)
	log.Info("scan and sbom jobs created")

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	statuses := make(chan string, 2)

	wg.Add(1)
	go func() {
		defer wg.Done()
		phaseStart := time.Now()
		if err := s.waitForJob(ctx, scanJobName, time.Duration(scanDeadline+60)*time.Second); err != nil {
			recordBuildPhase("scan", phaseStart, "error")
			metrics.Counter("build_jobs_failed_total", "Build jobs failed by mode.", metrics.Labels("mode", "scan"), 1)
			errs <- fmt.Errorf("scan: %w", err)
			return
		}
		report, _ := s.readJobLogs(ctx, scanJobName, "")
		_ = s.minio.UploadBytes(ctx, "submissions/"+id+"/trivy-report.json", "application/json", report)
		recordBuildPhase("scan", phaseStart, "ok")
		statuses <- topics.StatusScanned
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		phaseStart := time.Now()
		if err := s.waitForJob(ctx, sbomJobName, time.Duration(sbomDeadline+60)*time.Second); err != nil {
			recordBuildPhase("sbom", phaseStart, "error")
			metrics.Counter("build_jobs_failed_total", "Build jobs failed by mode.", metrics.Labels("mode", "sbom"), 1)
			errs <- fmt.Errorf("sbom: %w", err)
			return
		}
		sbom, _ := s.readJobLogs(ctx, sbomJobName, "")
		_ = s.minio.UploadBytes(ctx, "submissions/"+id+"/sbom.json", "application/json", sbom)
		recordBuildPhase("sbom", phaseStart, "ok")
		statuses <- topics.StatusSBOMReady
	}()

	go func() {
		wg.Wait()
		close(statuses)
		close(errs)
	}()

	oks, err := phase2Outcome(statuses, errs)
	if err != nil {
		log.Error("phase 2 job failed", "error", err)
		recordBuildRequest("error")
		s.setStatus(ctx, id, topics.StatusFailed, err.Error())
		return
	}
	for _, st := range oks {
		s.setStatus(ctx, id, st, st+" complete")
		log.Info("phase 2 step complete", "status", st)
	}
	log.Info("phase 2 complete")

	productionRef := fmt.Sprintf("%s/%s/%s:latest", s.cfg.HarborProductionEndpoint, s.cfg.HarborProject, id)
	phaseStart = time.Now()
	if err := s.ensureRepository(ctx, productionRef); err != nil {
		recordBuildPhase("promote", phaseStart, "error")
		recordBuildRequest("error")
		log.Error("ensure production repository failed", "error", err)
		s.setStatus(ctx, id, topics.StatusFailed, fmt.Sprintf("ensure production repository: %v", err))
		return
	}
	auth := crane.WithAuth(authn.FromConfig(authn.AuthConfig{
		Username: s.cfg.HarborUser,
		Password: s.cfg.HarborPassword,
	}))
	copyOptions := []crane.Option{auth, crane.WithContext(ctx)}
	if s.registryInsecure(s.cfg.HarborStagingEndpoint) || s.registryInsecure(s.cfg.HarborProductionEndpoint) {
		copyOptions = append(copyOptions, crane.Insecure)
	}
	if err := crane.Copy(stagingRef, productionRef, copyOptions...); err != nil {
		recordBuildPhase("promote", phaseStart, "error")
		metrics.Counter("harbor_promote_total", "Harbor image promotions by result.", metrics.Labels("result", "error"), 1)
		recordBuildRequest("error")
		log.Error("harbor promote failed", "error", err)
		s.setStatus(ctx, id, topics.StatusFailed, fmt.Sprintf("promote staging→production: %v", err))
		return
	}
	recordBuildPhase("promote", phaseStart, "ok")
	metrics.Counter("harbor_promote_total", "Harbor image promotions by result.", metrics.Labels("result", "ok"), 1)
	log.Info("image promoted to production", "ref", productionRef)

	if err := s.updater.UpdateImageRef(ctx, id, productionRef); err != nil {
		recordBuildRequest("error")
		log.Error("persist image ref failed", "error", err)
		s.setStatus(ctx, id, topics.StatusFailed, fmt.Sprintf("persist image ref: %v", err))
		return
	}

	s.setStatus(ctx, id, topics.StatusReady, "image ready")
	recordBuildRequest("ok")
	metrics.Histogram("build_request_duration_seconds", "End-to-end build request duration in seconds.", metrics.Labels("result", "ok"), metrics.SinceSeconds(runStart))
	log.Info("pipeline complete")
}

// phase2Outcome performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func phase2Outcome(statuses <-chan string, errs <-chan error) ([]string, error) {
	var oks []string
	for st := range statuses {
		oks = append(oks, st)
	}
	for err := range errs {
		if err != nil {
			return nil, err
		}
	}
	return oks, nil
}

// createJob applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (s *Spawner) createJob(ctx context.Context, job *batchv1.Job) error {
	_, err := s.client.BatchV1().Jobs(s.cfg.Namespace).Create(ctx, job, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		s.log.Info("job already exists; adopting (redelivery)", "job", job.Name)
		return nil
	}
	return err
}

// waitForJob applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
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

// readJobLogs applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
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

// buildJobSpec applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (s *Spawner) buildJobSpec(jobName string, msg topics.SubmissionBuildRequested, stagingRef, dockerfileB64 string) *batchv1.Job {
	ttl := jobTTL
	deadline := buildDeadline
	backoff := int32(0)
	kanikoArgs := []string{
		"--context=dir:///workspace",
		"--dockerfile=/workspace/Dockerfile",
		"--destination=" + stagingRef,
		"--ignore-path=/product_uuid",
	}
	if s.registryInsecure(s.cfg.HarborStagingEndpoint) {
		kanikoArgs = append(kanikoArgs, "--insecure-registry="+s.cfg.HarborStagingEndpoint)
	}

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
					RestartPolicy:   corev1.RestartPolicyNever,
					SecurityContext: podSecurityContext(),
					Tolerations:     s.buildTolerations(),
					NodeSelector:    s.buildNodeSelector(),
					Volumes: []corev1.Volume{
						{
							Name:         "workspace",
							VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
						},
						{
							Name:         "kaniko-config",
							VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
						},
					},
					InitContainers: []corev1.Container{
						{
							Name:            "fetch",
							Image:           s.cfg.SpawnerImage,
							Command:         []string{"/usr/local/bin/fetcher"},
							Env:             s.fetcherEnv(msg, dockerfileB64),
							SecurityContext: containerSecurityContext(),
							VolumeMounts: []corev1.VolumeMount{
								{Name: "workspace", MountPath: "/workspace"},
								{Name: "kaniko-config", MountPath: "/kaniko/.docker"},
							},
						},
					},
					Containers: []corev1.Container{
						{
							Name:            "build",
							Image:           s.cfg.KanikoImage,
							Args:            kanikoArgs,
							SecurityContext: kanikoSecurityContext(),
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

// scanJobSpec applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (s *Spawner) scanJobSpec(jobName, submissionID, stagingRef string) *batchv1.Job {
	ttl := jobTTL
	deadline := scanDeadline
	backoff := int32(0)
	scanArgs := []string{
		"image",
		"--format", "json",
		"--quiet",
	}
	if s.registryInsecure(s.cfg.HarborStagingEndpoint) {
		scanArgs = append(scanArgs, "--insecure")
	}
	scanArgs = append(scanArgs, stagingRef)

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
					RestartPolicy:   corev1.RestartPolicyNever,
					SecurityContext: podSecurityContext(),
					Tolerations:     s.buildTolerations(),
					NodeSelector:    s.buildNodeSelector(),
					Volumes: []corev1.Volume{
						{Name: "tmp", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
					},
					Containers: []corev1.Container{
						{
							Name:  "scan",
							Image: s.cfg.TrivyImage,
							Args:  scanArgs,
							Env: []corev1.EnvVar{
								{Name: "TRIVY_USERNAME", ValueFrom: s.secretKeyRef("harbor-user")},
								{Name: "TRIVY_PASSWORD", ValueFrom: s.secretKeyRef("harbor-password")},
								{Name: "TRIVY_CACHE_DIR", Value: "/tmp/trivy-cache"},
							},
							VolumeMounts: []corev1.VolumeMount{
								{Name: "tmp", MountPath: "/tmp"},
							},
							SecurityContext: containerSecurityContext(),
						},
					},
				},
			},
		},
	}
}

// sbomJobSpec applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (s *Spawner) sbomJobSpec(jobName, submissionID, stagingRef string) *batchv1.Job {
	ttl := jobTTL
	deadline := sbomDeadline
	backoff := int32(0)
	authority := strings.SplitN(stagingRef, "/", 2)[0]
	sbomEnv := []corev1.EnvVar{
		{Name: "SYFT_REGISTRY_AUTH_AUTHORITY", Value: authority},
		{Name: "SYFT_REGISTRY_AUTH_USERNAME", ValueFrom: s.secretKeyRef("harbor-user")},
		{Name: "SYFT_REGISTRY_AUTH_PASSWORD", ValueFrom: s.secretKeyRef("harbor-password")},
		{Name: "TMPDIR", Value: "/tmp"},
		{Name: "XDG_CACHE_HOME", Value: "/tmp/.cache"},
	}
	if s.registryInsecure(s.cfg.HarborStagingEndpoint) {
		sbomEnv = append(sbomEnv,
			corev1.EnvVar{Name: "SYFT_REGISTRY_INSECURE_USE_HTTP", Value: "true"},
			corev1.EnvVar{Name: "SYFT_REGISTRY_INSECURE_SKIP_TLS_VERIFY", Value: "true"},
		)
	}

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
					RestartPolicy:   corev1.RestartPolicyNever,
					SecurityContext: podSecurityContext(),
					Tolerations:     s.buildTolerations(),
					NodeSelector:    s.buildNodeSelector(),
					Volumes: []corev1.Volume{
						{Name: "tmp", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
					},
					Containers: []corev1.Container{
						{
							Name:  "sbom",
							Image: s.cfg.SyftImage,
							Args: []string{
								"registry:" + stagingRef,
								"-o", "spdx-json",
								"-q",
							},
							Env: sbomEnv,
							VolumeMounts: []corev1.VolumeMount{
								{Name: "tmp", MountPath: "/tmp"},
							},
							SecurityContext: containerSecurityContext(),
						},
					},
				},
			},
		},
	}
}

// fetcherEnv applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (s *Spawner) fetcherEnv(msg topics.SubmissionBuildRequested, dockerfileB64 string) []corev1.EnvVar {
	return []corev1.EnvVar{
		{Name: "MINIO_ENDPOINT", Value: s.cfg.MinioEndpoint},
		{Name: "MINIO_ACCESS_KEY", ValueFrom: s.secretKeyRef("minio-access-key")},
		{Name: "MINIO_SECRET_KEY", ValueFrom: s.secretKeyRef("minio-secret-key")},
		{Name: "MINIO_BUCKET", Value: s.cfg.MinioBucket},
		{Name: "ARTIFACT_PATH", Value: msg.ArtifactPath},
		{Name: "HARBOR_STAGING_ENDPOINT", ValueFrom: s.secretKeyRef("harbor-staging-endpoint")},
		{Name: "HARBOR_USER", ValueFrom: s.secretKeyRef("harbor-user")},
		{Name: "HARBOR_PASSWORD", ValueFrom: s.secretKeyRef("harbor-password")},
		{Name: "DOCKERFILE_B64", Value: dockerfileB64},
	}
}

// secretKeyRef applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (s *Spawner) secretKeyRef(key string) *corev1.EnvVarSource {
	return &corev1.EnvVarSource{
		SecretKeyRef: &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: s.cfg.JobSecretName},
			Key:                  key,
		},
	}
}

// setStatus applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (s *Spawner) setStatus(_ context.Context, submissionID, status, message string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := s.updater.PublishStatus(ctx, submissionID, status, message); err != nil {
		metrics.Counter("submission_status_transition_total", "Submission status transitions by status/result.", metrics.Labels("status", status, "result", "publish_error"), 1)
		s.log.Warn("publish status failed", "error", err)
	}
	if err := s.updater.UpdateDBStatus(ctx, submissionID, status, message); err != nil {
		metrics.Counter("submission_status_transition_total", "Submission status transitions by status/result.", metrics.Labels("status", status, "result", "db_error"), 1)
		s.log.Warn("db status update failed", "error", err)
	}
	metrics.Counter("submission_status_transition_total", "Submission status transitions by status/result.", metrics.Labels("status", status, "result", "ok"), 1)
}

// buildTolerations applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
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

// buildNodeSelector applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (s *Spawner) buildNodeSelector() map[string]string {
	if s.cfg.BuildNodePool == "" {
		return nil
	}
	return map[string]string{"pool": s.cfg.BuildNodePool}
}

// isJobComplete performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func isJobComplete(job *batchv1.Job) bool {
	for _, c := range job.Status.Conditions {
		if c.Type == batchv1.JobComplete && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

// isJobFailed performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func isJobFailed(job *batchv1.Job) bool {
	for _, c := range job.Status.Conditions {
		if c.Type == batchv1.JobFailed && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

// jobFailureMessage performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func jobFailureMessage(job *batchv1.Job) string {
	for _, c := range job.Status.Conditions {
		if c.Type == batchv1.JobFailed {
			return c.Message
		}
	}
	return "job failed"
}

// loadK8sConfig performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func loadK8sConfig() (*rest.Config, error) {
	cfg, inClusterErr := rest.InClusterConfig()
	if inClusterErr == nil {
		return cfg, nil
	}
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	cfg, kubeconfigErr := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, nil).ClientConfig()
	if kubeconfigErr != nil {
		return nil, fmt.Errorf("load k8s config: in-cluster config failed: %v; kubeconfig failed: %w", inClusterErr, kubeconfigErr)
	}
	return cfg, nil
}

// resourceName performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func resourceName(prefix, submissionID string) string {
	hash := sha256.Sum256([]byte(submissionID))
	hashSuffix := hex.EncodeToString(hash[:])[:10]
	suffixBudget := maxK8sNameLen - len(prefix) - len(hashSuffix) - 2
	if suffixBudget < 1 {
		suffixBudget = 1
	}

	safe := dnsLabelFragment(submissionID)
	if len(safe) > suffixBudget {
		safe = strings.Trim(safe[:suffixBudget], "-")
	}
	if safe == "" {
		safe = "submission"
	}
	return fmt.Sprintf("%s-%s-%s", prefix, safe, hashSuffix)
}

// dnsLabelFragment performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func dnsLabelFragment(value string) string {
	var b strings.Builder
	lastHyphen := false
	for i := 0; i < len(value); i++ {
		c := value[i]
		switch {
		case c >= 'a' && c <= 'z':
			b.WriteByte(c)
			lastHyphen = false
		case c >= '0' && c <= '9':
			b.WriteByte(c)
			lastHyphen = false
		case c >= 'A' && c <= 'Z':
			b.WriteByte(c + ('a' - 'A'))
			lastHyphen = false
		default:
			if !lastHyphen {
				b.WriteByte('-')
				lastHyphen = true
			}
		}
	}
	return strings.Trim(b.String(), "-")
}

// podSecurityContext performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func podSecurityContext() *corev1.PodSecurityContext {
	return &corev1.PodSecurityContext{
		RunAsNonRoot: boolPtr(true),
	}
}

// containerSecurityContext performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func containerSecurityContext() *corev1.SecurityContext {
	return &corev1.SecurityContext{
		RunAsNonRoot:             boolPtr(true),
		RunAsUser:                int64Ptr(65532),
		AllowPrivilegeEscalation: boolPtr(false),
		ReadOnlyRootFilesystem:   boolPtr(true),
		Capabilities: &corev1.Capabilities{
			Drop: []corev1.Capability{"ALL"},
		},
	}
}

// kanikoSecurityContext performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func kanikoSecurityContext() *corev1.SecurityContext {
	return &corev1.SecurityContext{
		RunAsNonRoot:             boolPtr(false),
		RunAsUser:                int64Ptr(0),
		ReadOnlyRootFilesystem:   boolPtr(false),
		AllowPrivilegeEscalation: boolPtr(false),
	}
}

// boolPtr performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func boolPtr(v bool) *bool {
	return &v
}

// int64Ptr performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func int64Ptr(v int64) *int64 {
	return &v
}

// registryInsecure applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (s *Spawner) registryInsecure(endpoint string) bool {
	return s.cfg.RegistryInsecure || isLocalRegistry(endpoint)
}

// isLocalRegistry performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func isLocalRegistry(endpoint string) bool {
	endpoint = strings.TrimSpace(strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://"))
	return strings.HasPrefix(endpoint, "localhost:") ||
		strings.HasPrefix(endpoint, "127.0.0.1:") ||
		strings.Contains(endpoint, "kind-registry")
}

// ensureRepository applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (s *Spawner) ensureRepository(ctx context.Context, imageRef string) error {
	if s.ecr == nil {
		return nil
	}
	repo := ecrRepositoryName(imageRef)
	if err := s.ecr.CreateRepository(ctx, repo); err != nil {
		if isECRRepositoryAlreadyExists(err) {
			s.log.Info("ecr repository already exists; adopting", "repository", repo)
			return nil
		}
		return fmt.Errorf("create ecr repository %s: %w", repo, err)
	}
	s.log.Info("ecr repository created", "repository", repo)
	return nil
}

// ecrRepositoryName performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func ecrRepositoryName(imageRef string) string {
	path := imageRef
	if i := strings.Index(path, "/"); i >= 0 {
		path = path[i+1:]
	}
	if i := strings.LastIndex(path, ":"); i > strings.LastIndex(path, "/") {
		path = path[:i]
	}
	return path
}

// isECRRepositoryAlreadyExists performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func isECRRepositoryAlreadyExists(err error) bool {
	var apiErr interface{ ErrorCode() string }
	return errors.As(err, &apiErr) && apiErr.ErrorCode() == "RepositoryAlreadyExistsException"
}

// recordBuildPhase performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func recordBuildPhase(phase string, start time.Time, result string) {
	labels := metrics.Labels("phase", phase, "result", result)
	metrics.Counter("build_phase_total", "Build phases by phase and result.", labels, 1)
	metrics.Histogram("build_phase_duration_seconds", "Build phase duration in seconds.", labels, metrics.SinceSeconds(start))
}

// recordBuildRequest performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func recordBuildRequest(result string) {
	metrics.Counter("build_requests_total", "Build requests by result.", metrics.Labels("result", result), 1)
}
