package k8s

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"testing"

	"github.com/iicpc/schemas/topics"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
)

func bugfixSpawner(client kubernetes.Interface, updater StatusUpdater) *Spawner {
	return &Spawner{
		client: client,
		cfg: JobConfig{
			Namespace: "build", SpawnerImage: "spawner:dev", KanikoImage: "kaniko:dev",
			TrivyImage: "trivy:dev", SyftImage: "syft:dev",
			HarborStagingEndpoint: "h", HarborProject: "iicpc",
		},
		updater: updater,
		log:     slog.Default(),
	}
}

func bfEnv(c corev1.Container, key string) (string, bool) {
	for _, e := range c.Env {
		if e.Name == key {
			return e.Value, true
		}
	}
	return "", false
}

func bfMount(c corev1.Container, name string) (string, bool) {
	for _, m := range c.VolumeMounts {
		if m.Name == name {
			return m.MountPath, true
		}
	}
	return "", false
}

func bfHasVolume(vs []corev1.Volume, name string) bool {
	for _, v := range vs {
		if v.Name == name {
			return true
		}
	}
	return false
}

// C3: every hardened container must run as an explicit non-root UID so the
// pod-level RunAsNonRoot=true passes admission regardless of the image's USER.
func TestContainerSecurityContextNonRoot(t *testing.T) {
	sc := containerSecurityContext()
	if sc.RunAsNonRoot == nil || !*sc.RunAsNonRoot {
		t.Error("RunAsNonRoot must be true")
	}
	if sc.RunAsUser == nil || *sc.RunAsUser != 65532 {
		t.Errorf("RunAsUser = %v, want 65532", sc.RunAsUser)
	}

	s := bugfixSpawner(fake.NewSimpleClientset(), nil)
	fetch := s.buildJobSpec("b", topics.SubmissionBuildRequested{SubmissionID: "sub"}, "ref", "b64").
		Spec.Template.Spec.InitContainers[0]
	if fetch.SecurityContext == nil || fetch.SecurityContext.RunAsUser == nil || *fetch.SecurityContext.RunAsUser != 65532 {
		t.Errorf("fetch init container RunAsUser not 65532: %+v", fetch.SecurityContext)
	}
}

// H7: trivy and syft run with ReadOnlyRootFilesystem=true, so they need a
// writable scratch volume + cache env or they abort on a cold pod.
func TestScanSbomHaveWritableScratch(t *testing.T) {
	s := bugfixSpawner(fake.NewSimpleClientset(), nil)

	scan := s.scanJobSpec("scan", "sub", "ref")
	if !bfHasVolume(scan.Spec.Template.Spec.Volumes, "tmp") {
		t.Error("scan job missing writable tmp volume")
	}
	if mp, ok := bfMount(scan.Spec.Template.Spec.Containers[0], "tmp"); !ok || mp != "/tmp" {
		t.Errorf("scan container tmp mount = %q,%v, want /tmp", mp, ok)
	}
	if v, ok := bfEnv(scan.Spec.Template.Spec.Containers[0], "TRIVY_CACHE_DIR"); !ok || v == "" {
		t.Error("scan container missing TRIVY_CACHE_DIR pointing at writable scratch")
	}

	sbom := s.sbomJobSpec("sbom", "sub", "ref")
	if !bfHasVolume(sbom.Spec.Template.Spec.Volumes, "tmp") {
		t.Error("sbom job missing writable tmp volume")
	}
	if v, ok := bfEnv(sbom.Spec.Template.Spec.Containers[0], "TMPDIR"); !ok || v != "/tmp" {
		t.Errorf("sbom container TMPDIR = %q,%v, want /tmp", v, ok)
	}
}

func TestBuildJobAllowsInsecureKindRegistry(t *testing.T) {
	s := bugfixSpawner(fake.NewSimpleClientset(), nil)
	s.cfg.HarborStagingEndpoint = "kind-registry:5000"

	build := s.buildJobSpec("build-sub", topics.SubmissionBuildRequested{SubmissionID: "sub"}, "kind-registry:5000/iicpc/sub:latest", "b64")
	args := build.Spec.Template.Spec.Containers[0].Args
	if !containsString(args, "--insecure-registry=kind-registry:5000") {
		t.Fatalf("kaniko args missing insecure registry flag: %v", args)
	}
	if !containsString(args, "--ignore-path=/product_uuid") {
		t.Fatalf("kaniko args missing product_uuid ignore path: %v", args)
	}

	scan := s.scanJobSpec("scan-sub", "sub", "kind-registry:5000/iicpc/sub:latest")
	if !containsString(scan.Spec.Template.Spec.Containers[0].Args, "--insecure") {
		t.Fatalf("trivy args missing insecure flag: %v", scan.Spec.Template.Spec.Containers[0].Args)
	}

	sbom := s.sbomJobSpec("sbom-sub", "sub", "kind-registry:5000/iicpc/sub:latest")
	env := sbom.Spec.Template.Spec.Containers[0].Env
	if value, ok := bfEnv(corev1.Container{Env: env}, "SYFT_REGISTRY_INSECURE_USE_HTTP"); !ok || value != "true" {
		t.Fatalf("syft missing insecure http env, got %q present=%v", value, ok)
	}
}

// FIX 2 (audit M-insecure-registry): the old RFC1918 heuristic silently
// stripped TLS from kaniko/trivy/syft/crane on EKS VPC and service CIDRs.
// Insecure registry access is now an explicit opt-in (REGISTRY_INSECURE);
// only loopback and kind-registry endpoints stay auto-insecure for dev.
func TestRegistryInsecureIsExplicitNotHeuristic(t *testing.T) {
	tests := []struct {
		name     string
		endpoint string
		flag     bool
		want     bool
	}{
		{name: "10.x secure by default", endpoint: "10.100.0.5:5000", want: false},
		{name: "172.x secure by default", endpoint: "172.20.0.3:5000", want: false},
		{name: "192.168.x secure by default", endpoint: "192.168.1.10:5000", want: false},
		{name: "ecr endpoint secure", endpoint: "123456789012.dkr.ecr.us-east-1.amazonaws.com", want: false},
		{name: "localhost convenience", endpoint: "localhost:5000", want: true},
		{name: "loopback convenience", endpoint: "127.0.0.1:5000", want: true},
		{name: "kind-registry convenience", endpoint: "kind-registry:5000", want: true},
		{name: "scheme-prefixed loopback", endpoint: "http://127.0.0.1:5000", want: true},
		{name: "explicit flag covers private IP", endpoint: "10.100.0.5:5000", flag: true, want: true},
		{name: "explicit flag covers anything", endpoint: "harbor.example.com", flag: true, want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := bugfixSpawner(fake.NewSimpleClientset(), nil)
			s.cfg.RegistryInsecure = tt.flag
			if got := s.registryInsecure(tt.endpoint); got != tt.want {
				t.Fatalf("registryInsecure(%q) flag=%v = %v, want %v", tt.endpoint, tt.flag, got, tt.want)
			}
		})
	}
}

// Job specs must not downgrade TLS for private-CIDR endpoints unless the
// explicit env is set — production EKS registries live on exactly these CIDRs.
func TestJobSpecsSecureByDefaultOnPrivateCIDRs(t *testing.T) {
	s := bugfixSpawner(fake.NewSimpleClientset(), nil)
	s.cfg.HarborStagingEndpoint = "10.100.0.5:5000"

	build := s.buildJobSpec("build-sub", topics.SubmissionBuildRequested{SubmissionID: "sub"}, "10.100.0.5:5000/iicpc/sub:latest", "b64")
	if containsString(build.Spec.Template.Spec.Containers[0].Args, "--insecure-registry=10.100.0.5:5000") {
		t.Fatalf("kaniko args downgraded TLS for a private CIDR without REGISTRY_INSECURE: %v", build.Spec.Template.Spec.Containers[0].Args)
	}

	scan := s.scanJobSpec("scan-sub", "sub", "10.100.0.5:5000/iicpc/sub:latest")
	if containsString(scan.Spec.Template.Spec.Containers[0].Args, "--insecure") {
		t.Fatalf("trivy args downgraded TLS for a private CIDR without REGISTRY_INSECURE: %v", scan.Spec.Template.Spec.Containers[0].Args)
	}

	sbom := s.sbomJobSpec("sbom-sub", "sub", "10.100.0.5:5000/iicpc/sub:latest")
	if _, ok := bfEnv(sbom.Spec.Template.Spec.Containers[0], "SYFT_REGISTRY_INSECURE_USE_HTTP"); ok {
		t.Fatal("syft env downgraded TLS for a private CIDR without REGISTRY_INSECURE")
	}
}

// FIX 1 (audit 1.9): ECR has no push-to-create — the spawner derives the
// repository path from the refs it composes (<endpoint>/<project>/<id>:latest)
// and pre-creates it before kaniko pushes / crane promotes.
func TestECRRepositoryName(t *testing.T) {
	tests := []struct {
		name string
		ref  string
		want string
	}{
		{name: "ecr staging ref", ref: "123456789012.dkr.ecr.us-east-1.amazonaws.com/iicpc/sub-42:latest", want: "iicpc/sub-42"},
		{name: "registry host with port", ref: "localhost:5000/iicpc/sub-42:latest", want: "iicpc/sub-42"},
		{name: "no tag", ref: "123456789012.dkr.ecr.us-east-1.amazonaws.com/iicpc/sub-42", want: "iicpc/sub-42"},
		{name: "nested project path", ref: "reg.example.com/team/project/sub:v1", want: "team/project/sub"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ecrRepositoryName(tt.ref); got != tt.want {
				t.Fatalf("ecrRepositoryName(%q) = %q, want %q", tt.ref, got, tt.want)
			}
		})
	}
}

// fakeECRClient mocks ECRRepositoryClient the same way the kubernetes fakes
// stand in for the real clientset.
type fakeECRClient struct {
	created []string
	err     error
}

func (f *fakeECRClient) CreateRepository(_ context.Context, repositoryName string) error {
	f.created = append(f.created, repositoryName)
	return f.err
}

// ecrAlreadyExistsErr mimics aws-sdk-go-v2's
// types.RepositoryAlreadyExistsException: a smithy APIError whose ErrorCode is
// the exception type name.
type ecrAlreadyExistsErr struct{}

func (ecrAlreadyExistsErr) Error() string     { return "repository already exists" }
func (ecrAlreadyExistsErr) ErrorCode() string { return "RepositoryAlreadyExistsException" }

func TestEnsureRepositoryToleratesAlreadyExists(t *testing.T) {
	tests := []struct {
		name      string
		createErr error
		wantErr   bool
	}{
		{name: "created fresh", createErr: nil, wantErr: false},
		{name: "already exists is success", createErr: ecrAlreadyExistsErr{}, wantErr: false},
		{name: "wrapped already exists is success", createErr: fmt.Errorf("create: %w", ecrAlreadyExistsErr{}), wantErr: false},
		{name: "other error propagates", createErr: errors.New("AccessDeniedException: not authorized"), wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ecrFake := &fakeECRClient{err: tt.createErr}
			s := bugfixSpawner(fake.NewSimpleClientset(), nil)
			s.cfg.RegistryProvider = "ecr"
			s.ecr = ecrFake

			err := s.ensureRepository(context.Background(), "123456789012.dkr.ecr.us-east-1.amazonaws.com/iicpc/sub-1:latest")
			if (err != nil) != tt.wantErr {
				t.Fatalf("ensureRepository error = %v, wantErr %v", err, tt.wantErr)
			}
			if len(ecrFake.created) != 1 || ecrFake.created[0] != "iicpc/sub-1" {
				t.Fatalf("CreateRepository called with %v, want exactly [iicpc/sub-1]", ecrFake.created)
			}
		})
	}
}

// REGISTRY_PROVIDER unset (the default) must be a strict no-op: Harbor creates
// repositories on push, and no ECR client is configured.
func TestEnsureRepositoryNoopWhenProviderOff(t *testing.T) {
	s := bugfixSpawner(fake.NewSimpleClientset(), nil)
	if err := s.ensureRepository(context.Background(), "h/iicpc/sub:latest"); err != nil {
		t.Fatalf("ensureRepository with provider off must be a no-op, got %v", err)
	}
}

// H6: a redelivered build (deterministic Job name already exists within its TTL)
// must NOT force-fail the submission — createJob treats AlreadyExists as success.
func TestCreateJobIdempotentOnRedelivery(t *testing.T) {
	client := fake.NewSimpleClientset()
	s := bugfixSpawner(client, nil)
	ctx := context.Background()
	job := s.buildJobSpec("dup-build", topics.SubmissionBuildRequested{SubmissionID: "sub"}, "ref", "b64")

	if err := s.createJob(ctx, job); err != nil {
		t.Fatalf("first create: %v", err)
	}
	if err := s.createJob(ctx, job); err != nil {
		t.Fatalf("redelivery: createJob on an existing Job must return nil, got %v", err)
	}
}

// M30: status writes must survive a cancelled request ctx (SIGTERM mid-build),
// or the terminal status is lost and the row is stranded non-terminal.
func TestSetStatusSurvivesCancelledCtx(t *testing.T) {
	rec := &recordingUpdater{}
	s := bugfixSpawner(fake.NewSimpleClientset(), rec)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // simulate shutdown-cancelled request ctx

	s.setStatus(ctx, "sub", topics.StatusFailed, "boom")

	if !rec.published || !rec.dbWritten {
		t.Fatalf("setStatus dropped writes: published=%v dbWritten=%v", rec.published, rec.dbWritten)
	}
	if rec.pubCtxErr != nil || rec.dbCtxErr != nil {
		t.Errorf("setStatus used the cancelled ctx (pub=%v db=%v); must detach", rec.pubCtxErr, rec.dbCtxErr)
	}
}

type recordingUpdater struct {
	published, dbWritten bool
	pubCtxErr, dbCtxErr  error
}

func (r *recordingUpdater) PublishStatus(ctx context.Context, _, _, _ string) error {
	r.published = true
	r.pubCtxErr = ctx.Err()
	return nil
}

func (r *recordingUpdater) UpdateDBStatus(ctx context.Context, _, _, _ string) error {
	r.dbWritten = true
	r.dbCtxErr = ctx.Err()
	return nil
}

func (r *recordingUpdater) UpdateImageRef(context.Context, string, string) error {
	return nil
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
