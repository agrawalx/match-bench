package k8s

import (
	"strings"
	"testing"

	"github.com/iicpc/schemas/topics"
	corev1 "k8s.io/api/core/v1"
)

func TestResourceNameIsKubernetesSafeAndStable(t *testing.T) {
	tests := []struct {
		name         string
		prefix       string
		submissionID string
	}{
		{name: "short id", prefix: "build", submissionID: "abc"},
		{name: "uppercase and separators", prefix: "scan", submissionID: "Submission_ID/ABC.123"},
		{name: "long id", prefix: "sbom", submissionID: strings.Repeat("A", 200)},
		{name: "symbols only", prefix: "build", submissionID: "___"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			first := resourceName(tt.prefix, tt.submissionID)
			second := resourceName(tt.prefix, tt.submissionID)
			if first != second {
				t.Fatalf("resourceName is not stable: %q != %q", first, second)
			}
			if len(first) > maxK8sNameLen {
				t.Fatalf("resourceName length = %d, want <= %d: %q", len(first), maxK8sNameLen, first)
			}
			if !strings.HasPrefix(first, tt.prefix+"-") {
				t.Fatalf("resourceName %q does not include prefix %q", first, tt.prefix)
			}
			for _, r := range first {
				if (r < 'a' || r > 'z') && (r < '0' || r > '9') && r != '-' {
					t.Fatalf("resourceName contains invalid DNS label character %q in %q", r, first)
				}
			}
			if strings.HasSuffix(first, "-") {
				t.Fatalf("resourceName must not end with hyphen: %q", first)
			}
		})
	}
}

func TestResourceNameAvoidsTruncationCollision(t *testing.T) {
	left := resourceName("build", strings.Repeat("a", 80)+"1")
	right := resourceName("build", strings.Repeat("a", 80)+"2")
	if left == right {
		t.Fatalf("resourceName collided for distinct submission IDs: %q", left)
	}
}

func TestJobSpecsUseSecretRefsForCredentials(t *testing.T) {
	spawner := &Spawner{cfg: JobConfig{Namespace: "build", JobSecretName: "spawner-secret"}}
	msg := topics.SubmissionBuildRequested{
		SubmissionID: "sub-123",
		ArtifactPath: "submissions/sub-123/artifact.zip",
	}

	build := spawner.buildJobSpec("build-sub-123", msg, "registry.example/iicpc/sub-123:latest", "ZG9ja2Vy")
	fetchEnv := build.Spec.Template.Spec.InitContainers[0].Env
	assertSecretEnv(t, fetchEnv, "MINIO_ACCESS_KEY", "spawner-secret", "minio-access-key")
	assertSecretEnv(t, fetchEnv, "MINIO_SECRET_KEY", "spawner-secret", "minio-secret-key")
	assertSecretEnv(t, fetchEnv, "HARBOR_USER", "spawner-secret", "harbor-user")
	assertSecretEnv(t, fetchEnv, "HARBOR_PASSWORD", "spawner-secret", "harbor-password")
	assertLockedDownContainer(t, build.Spec.Template.Spec.InitContainers[0].SecurityContext)
	assertLockedDownContainer(t, build.Spec.Template.Spec.Containers[0].SecurityContext)

	scan := spawner.scanJobSpec("scan-sub-123", "sub-123", "registry.example/iicpc/sub-123:latest")
	scanContainer := scan.Spec.Template.Spec.Containers[0]
	for _, arg := range scanContainer.Args {
		if arg == "--username" || arg == "--password" {
			t.Fatal("scan args must not pass credentials on the command line")
		}
	}
	assertSecretEnv(t, scanContainer.Env, "TRIVY_USERNAME", "spawner-secret", "harbor-user")
	assertSecretEnv(t, scanContainer.Env, "TRIVY_PASSWORD", "spawner-secret", "harbor-password")
	assertLockedDownContainer(t, scanContainer.SecurityContext)

	sbom := spawner.sbomJobSpec("sbom-sub-123", "sub-123", "registry.example/iicpc/sub-123:latest")
	sbomContainer := sbom.Spec.Template.Spec.Containers[0]
	assertSecretEnv(t, sbomContainer.Env, "SYFT_REGISTRY_AUTH_USERNAME", "spawner-secret", "harbor-user")
	assertSecretEnv(t, sbomContainer.Env, "SYFT_REGISTRY_AUTH_PASSWORD", "spawner-secret", "harbor-password")
	assertLockedDownContainer(t, sbomContainer.SecurityContext)
}

func assertSecretEnv(t *testing.T, envs []corev1.EnvVar, name, secretName, key string) {
	t.Helper()
	for _, env := range envs {
		if env.Name != name {
			continue
		}
		if env.Value != "" {
			t.Fatalf("%s uses literal value", name)
		}
		if env.ValueFrom == nil || env.ValueFrom.SecretKeyRef == nil {
			t.Fatalf("%s does not use SecretKeyRef", name)
		}
		if got := env.ValueFrom.SecretKeyRef.Name; got != secretName {
			t.Fatalf("%s secret name = %q, want %q", name, got, secretName)
		}
		if got := env.ValueFrom.SecretKeyRef.Key; got != key {
			t.Fatalf("%s secret key = %q, want %q", name, got, key)
		}
		return
	}
	t.Fatalf("env %s not found", name)
}

func assertLockedDownContainer(t *testing.T, securityContext *corev1.SecurityContext) {
	t.Helper()
	if securityContext == nil {
		t.Fatal("missing container security context")
	}
	if securityContext.AllowPrivilegeEscalation == nil || *securityContext.AllowPrivilegeEscalation {
		t.Fatal("allowPrivilegeEscalation must be false")
	}
	if securityContext.ReadOnlyRootFilesystem == nil || !*securityContext.ReadOnlyRootFilesystem {
		t.Fatal("readOnlyRootFilesystem must be true")
	}
	if securityContext.Capabilities == nil || len(securityContext.Capabilities.Drop) != 1 || securityContext.Capabilities.Drop[0] != "ALL" {
		t.Fatal("container must drop all capabilities")
	}
}
