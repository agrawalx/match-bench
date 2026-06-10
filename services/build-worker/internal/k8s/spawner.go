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

// StatusUpdater publishes and persists monotonic submission state transitions.
type StatusUpdater interface {
	PublishStatus(ctx context.Context, submissionID, status, message string) error
	UpdateDBStatus(ctx context.Context, submissionID, status, message string) error
	UpdateImageRef(ctx context.Context, submissionID, imageRef string) error
}

// MinioClient provides artifact IO for per-submission build jobs.
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
	JobSecretName  string

	HarborStagingEndpoint    string // e.g. harbor-staging.example.com
	HarborProductionEndpoint string // e.g. harbor.example.com
	HarborProject            string // e.g. iicpc
	HarborUser               string
	HarborPassword           string

	// RegistryProvider selects registry-specific behavior. "" (default) means a
	// push-to-create registry (Harbor/ghcr) and nothing extra happens. "ecr"
	// pre-creates the staging and production repositories via the ECR API before
	// the kaniko Job pushes and before crane promotes — ECR has no
	// push-to-create, so without this every first push to
	// <endpoint>/<project>/<submissionID> fails outright.
	RegistryProvider string

	// RegistryInsecure, when true, downgrades registry access to plain HTTP /
	// skip-TLS in kaniko, trivy, syft, and crane. Explicit opt-in for local dev
	// registries only — never set it in production. Loopback and kind-registry
	// endpoints are always treated as insecure for dev convenience regardless
	// of this flag.
	RegistryInsecure bool

	// BuildNodePool, when non-empty, pins Job pods to nodes with pool=<value> label
	// and adds the build=true:NoSchedule toleration. Leave empty for single-node dev.
	BuildNodePool string
}

// ECRRepositoryClient is the narrow surface of the ECR API the spawner needs
// when RegistryProvider is "ecr": create a repository ahead of a push. The
// concrete implementation wraps the aws-sdk-go-v2 ecr client built from the
// default AWS credential chain (IRSA in-cluster) and must return SDK errors
// unwrapped so ensureRepository can match RepositoryAlreadyExistsException.
// Faked in tests the same way kubernetes.Interface is.
type ECRRepositoryClient interface {
	CreateRepository(ctx context.Context, repositoryName string) error
}

// newECRClient builds the ECRRepositoryClient used when REGISTRY_PROVIDER=ecr.
// It is a package variable so tests can stub it; the production value is the
// aws-sdk-go-v2 adapter in ecr_aws.go (default AWS credential chain — IRSA
// in-cluster).
var newECRClient = newAWSECRClient

type Spawner struct {
	client  kubernetes.Interface
	cfg     JobConfig
	minio   MinioClient
	updater StatusUpdater
	ecr     ECRRepositoryClient // nil unless cfg.RegistryProvider == "ecr"
	log     *slog.Logger
}

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
		// push-to-create registry (Harbor/ghcr) — nothing to pre-create
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

func (s *Spawner) Run(ctx context.Context, msg topics.SubmissionBuildRequested) {
	runStart := time.Now()
	id := msg.SubmissionID
	log := s.log.With("submission_id", id)
	stagingRef := fmt.Sprintf("%s/%s/%s:latest", s.cfg.HarborStagingEndpoint, s.cfg.HarborProject, id)

	// Pre-check: zip-slip scan before any Job is created
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

	// Generate a platform-controlled Dockerfile from the submission metadata.
	// Contestants never supply a Dockerfile — the platform controls the build environment.
	dockerfileContent, err := dockerfile.Generate(msg.Language, msg.BuildType, msg.BuildTarget, msg.Port)
	if err != nil {
		recordBuildRequest("error")
		log.Error("failed to generate dockerfile", "error", err)
		s.setStatus(ctx, id, topics.StatusFailed, fmt.Sprintf("generate dockerfile: %v", err))
		return
	}

	dockerfileB64 := base64.StdEncoding.EncodeToString([]byte(dockerfileContent))

	// Phase 1: fetcher init container extracts ZIP → kaniko builds and pushes to Harbor staging
	buildJobName := resourceName("build", id)
	phaseStart = time.Now()
	s.setStatus(ctx, id, topics.StatusBuilding, "building image")
	// ECR has no push-to-create: the staging repository must exist before the
	// kaniko Job pushes to it. No-op for push-to-create registries (Harbor).
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

	// Phase 2: Trivy scan + Syft SBOM run in parallel against the staging registry image
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

	// Phase 3: promote staging → production via crane.Copy (no extra Job)
	productionRef := fmt.Sprintf("%s/%s/%s:latest", s.cfg.HarborProductionEndpoint, s.cfg.HarborProject, id)
	phaseStart = time.Now()
	// ECR again: the production repository must exist before crane copies into it.
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

// phase2Outcome collects the results of the parallel scan + SBOM jobs. Both
// channels must already be closed (the waiter goroutine closes them after
// wg.Wait). It returns the per-step success statuses to publish ONLY when no
// job failed — a partial failure returns the first error and NO statuses, so
// the caller never publishes a sibling's forward-progress status (e.g.
// sbom_ready) for a submission that is actually failing. Draining statuses
// before checking errs is safe because both channels are closed; the success
// statuses are discarded on any error rather than emitted ahead of `failed`.
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

func (s *Spawner) createJob(ctx context.Context, job *batchv1.Job) error {
	_, err := s.client.BatchV1().Jobs(s.cfg.Namespace).Create(ctx, job, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		// Idempotent redelivery: the build offset is committed only after the
		// whole pipeline finishes, so a SIGTERM mid-build replays the message and
		// the deterministic Job name still exists within its TTL window. Adopt it
		// and wait, instead of force-failing — failing here would even overwrite a
		// submission that already built successfully (failed is the top status rank).
		s.log.Info("job already exists; adopting (redelivery)", "job", job.Name)
		return nil
	}
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
	kanikoArgs := []string{
		"--context=dir:///workspace",
		"--dockerfile=/workspace/Dockerfile",
		"--destination=" + stagingRef,
		// Some Kubernetes/containerd nodes expose /product_uuid inside the
		// executor rootfs. Kaniko can compile successfully, then fail cleaning
		// a multi-stage build with "device or resource busy" unless this host
		// path is excluded from snapshots and stage cleanup.
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

// scanJobSpec creates a trivy vulnerability scan Job against the staging registry image.
// JSON report is written to stdout and collected by the spawner via pod logs.
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
					// Writable scratch: the container has ReadOnlyRootFilesystem=true,
					// but trivy must write its vulnerability DB + temp files. Without
					// this emptyDir the scan aborts on a cold pod.
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

// sbomJobSpec creates a syft SBOM generation Job against the staging registry image.
// SPDX-JSON output is written to stdout and collected by the spawner via pod logs.
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
					// Writable scratch: ReadOnlyRootFilesystem=true, but syft extracts
					// image layers to a temp dir. Point TMPDIR/cache at this emptyDir.
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

func (s *Spawner) secretKeyRef(key string) *corev1.EnvVarSource {
	return &corev1.EnvVarSource{
		SecretKeyRef: &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: s.cfg.JobSecretName},
			Key:                  key,
		},
	}
}

func (s *Spawner) setStatus(_ context.Context, submissionID, status, message string) {
	// Status writes must survive shutdown: the request ctx is cancelled on
	// SIGTERM, and if the terminal 'failed'/'ready' publish + DB write rode that
	// cancelled ctx the row would be stranded non-terminal and the offset
	// uncommitted. Detach with a fresh bounded context.
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

// loadK8sConfig uses in-cluster config when running inside a pod, falling back
// to kubeconfig for local dev. This mirrors sandbox-orchestrator's loadConfig —
// the standard loader for every service in this repo (see invariant 6.8). The
// in-cluster path succeeds only with a mounted ServiceAccount token, so the
// kubeconfig fallback only fires out-of-cluster.
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

func podSecurityContext() *corev1.PodSecurityContext {
	return &corev1.PodSecurityContext{
		RunAsNonRoot: boolPtr(true),
	}
}

func containerSecurityContext() *corev1.SecurityContext {
	return &corev1.SecurityContext{
		// Explicit non-root UID: the pod sets RunAsNonRoot=true, which the kubelet
		// rejects unless a non-root UID is known. The fetch init container's image
		// (SpawnerImage) and the trivy/syft images may default to root, so pin the
		// UID here rather than relying on the image's USER directive.
		RunAsNonRoot:             boolPtr(true),
		RunAsUser:                int64Ptr(65532),
		AllowPrivilegeEscalation: boolPtr(false),
		ReadOnlyRootFilesystem:   boolPtr(true),
		Capabilities: &corev1.Capabilities{
			Drop: []corev1.Capability{"ALL"},
		},
	}
}

// kanikoSecurityContext is the per-container override for the kaniko build
// container only. The kaniko executor image runs as root (UID 0) and must
// extract base-image layers into the container root filesystem, chowning files
// to match the source image — so RunAsNonRoot, ReadOnlyRootFilesystem, and
// dropping all capabilities (CAP_CHOWN/CAP_DAC_OVERRIDE/CAP_FOWNER are needed
// for extraction) all break it. The container-level RunAsNonRoot:false
// overrides the pod-level RunAsNonRoot:true, so the sibling fetch init
// container stays hardened while only kaniko is relaxed.
// AllowPrivilegeEscalation stays false (kaniko is already root and does not
// need to gain new privileges).
func kanikoSecurityContext() *corev1.SecurityContext {
	return &corev1.SecurityContext{
		RunAsNonRoot:             boolPtr(false),
		RunAsUser:                int64Ptr(0),
		ReadOnlyRootFilesystem:   boolPtr(false),
		AllowPrivilegeEscalation: boolPtr(false),
	}
}

func boolPtr(v bool) *bool {
	return &v
}

func int64Ptr(v int64) *int64 {
	return &v
}

// registryInsecure reports whether registry access for endpoint may downgrade
// to plain HTTP / skipped TLS verification (kaniko --insecure-registry, trivy
// --insecure, syft SYFT_REGISTRY_INSECURE_*, crane.Insecure). Downgrading is
// an explicit opt-in via REGISTRY_INSECURE, plus loopback/kind-registry
// convenience matches ONLY. The old 10.*/172.*/192.168.* heuristic is gone
// deliberately: EKS pod/service CIDRs (and most in-VPC registries) live in
// exactly those ranges, so the heuristic silently stripped TLS in production.
func (s *Spawner) registryInsecure(endpoint string) bool {
	return s.cfg.RegistryInsecure || isLocalRegistry(endpoint)
}

func isLocalRegistry(endpoint string) bool {
	endpoint = strings.TrimSpace(strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://"))
	return strings.HasPrefix(endpoint, "localhost:") ||
		strings.HasPrefix(endpoint, "127.0.0.1:") ||
		strings.Contains(endpoint, "kind-registry")
}

// ensureRepository pre-creates the repository behind imageRef when the
// registry provider requires it. ECR has no push-to-create: a kaniko push or
// crane copy into a nonexistent repository fails outright, so the spawner
// creates <project>/<submissionID> ahead of both. RepositoryAlreadyExists is
// success — refs are deterministic, so every rebuild of a submission hits an
// existing repository. No-op when REGISTRY_PROVIDER is unset (push-to-create
// registries like Harbor need nothing).
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

// ecrRepositoryName derives the ECR repositoryName from an image ref the
// spawner composed (<endpoint>/<project>/<submissionID>[:tag]): drop the
// registry host (first path segment) and the tag. The tag colon is only
// stripped when it appears after the last slash, so registry ports
// (localhost:5000/...) never eat into the path.
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

// isECRRepositoryAlreadyExists matches aws-sdk-go-v2's
// types.RepositoryAlreadyExistsException without importing the SDK here: all
// AWS API errors implement smithy APIError's ErrorCode, and the exception's
// code is its type name.
func isECRRepositoryAlreadyExists(err error) bool {
	var apiErr interface{ ErrorCode() string }
	return errors.As(err, &apiErr) && apiErr.ErrorCode() == "RepositoryAlreadyExistsException"
}

// recordBuildPhase and recordBuildRequest expose build pipeline health.
func recordBuildPhase(phase string, start time.Time, result string) {
	labels := metrics.Labels("phase", phase, "result", result)
	metrics.Counter("build_phase_total", "Build phases by phase and result.", labels, 1)
	metrics.Histogram("build_phase_duration_seconds", "Build phase duration in seconds.", labels, metrics.SinceSeconds(start))
}

func recordBuildRequest(result string) {
	metrics.Counter("build_requests_total", "Build requests by result.", metrics.Labels("result", result), 1)
}
