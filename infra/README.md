# match-bench EKS Infrastructure-as-Code

This directory automates the EKS bring-up that `DEPLOYMENT_EKS.md` describes as a
manual runbook. It is the same cluster — same node groups, same kubelet tuning,
same addons — expressed as Terraform + a Makefile so a second environment is one
`terraform apply`, not a copy-paste of shell exports.

```
infra/
  terraform/          # cluster, ECR, IRSA, addons (the "what AWS holds")
    versions.tf       # provider + module pins
    variables.tf      # region, cluster_name, account_id, instance types, sizes
    main.tf           # providers, VPC, EKS cluster + 2 node groups + 4 addons
    ecr.tf            # 12 ECR repos (one per service image)
    irsa.tf           # IRSA roles: EBS CSI, ALB controller, build-spawner
    addons.tf         # gp3 StorageClass, ALB controller, KEDA, gVisor RuntimeClass
    outputs.tf        # cluster endpoint, ECR URLs, kubeconfig command
    terraform.tfvars.example
  Makefile            # init/plan/apply/kubeconfig/ecr-login/images/deploy/destroy
  scripts/
    install-addons.sh # fallback addon installer (if you ran TF without helm)
    netpol-deny-test.sh  # the HARD GATE: proves sandbox isolation is enforced
```

## What is automated vs. manual

| Concern | Automated by | Notes |
|---|---|---|
| VPC (3 AZ, NAT, ELB discovery tags) | `terraform apply` | `main.tf` module.vpc |
| EKS control plane, version 1.32 | `terraform apply` | `main.tf` module.eks |
| OIDC provider for IRSA | `terraform apply` | `enable_irsa = true` |
| `general` node group (m6i, min2/max4) | `terraform apply` | default kubelet |
| `sandbox` node group (c6i.2xlarge, tainted, AL2023, static CPU manager + full-pcpus-only + reservedSystemCPUs 0,1) | `terraform apply` | kubelet via AL2023 NodeConfig |
| addons: vpc-cni (NetworkPolicy ON), kube-proxy, coredns, aws-ebs-csi-driver | `terraform apply` | `cluster_addons` |
| 12 ECR repos (IMMUTABLE) | `terraform apply` | `ecr.tf` |
| IRSA: EBS CSI, ALB controller, build-spawner (ECR create/describe/push) | `terraform apply` | `irsa.tf` |
| gp3 default StorageClass | `terraform apply` | `addons.tf` (k8s provider) |
| AWS Load Balancer Controller, KEDA, gVisor RuntimeClass object | `terraform apply` | `addons.tf` (helm + k8s) |
| Build + push 12 images by git SHA | `make images` | tags by `git rev-parse --short HEAD` |
| Apply `k8s/` in dependency order + image substitution | `make deploy` | order from DEPLOYMENT §6/§12 |
| **Secrets** | **MANUAL** | from AWS Secrets Manager — see below |
| **runsc (gVisor) host install on sandbox nodes** | **MANUAL** | if you use gVisor — see below |
| **Frontend ALB Ingress (needs your ACM cert ARN)** | **MANUAL** | see below |
| **NetworkPolicy enforcement gate** | `make netpol-gate` | run it; do not skip |

## Prerequisites

`awscli v2`, `terraform >= 1.6`, `kubectl`, `helm v3`, `docker`, and an AWS
principal that can create EKS clusters, EC2 node groups, IAM roles, ECR repos,
and ELBs. Configure credentials (`aws configure` / SSO) before starting.

## End to end

```bash
cd infra

# 1. Cluster, ECR, IRSA, addons. (~15-20 min for the EKS control plane + nodes.)
cp terraform/terraform.tfvars.example terraform/terraform.tfvars   # edit region/sizes
make init
make plan
# First apply: bring up the cluster before the k8s/helm providers read the API.
# The gVisor RuntimeClass uses kubernetes_manifest, which does a plan-time API
# read that does not exist until the cluster does — so target the cluster first:
cd terraform && terraform apply -target=module.eks && cd ..
make apply         # full apply: ECR, IRSA, StorageClass, ALB controller, KEDA, gVisor

# 2. Point kubectl at it.
make kubeconfig

# 3. MANUAL: create secrets (see "Manual steps" below) before deploying apps.

# 4. Build + push all 12 images by git SHA, then deploy k8s/ in order.
make images        # docker build+push -> <acct>.dkr.ecr.<region>.amazonaws.com/iicpc/*:<sha>
make deploy        # kubectl apply k8s/ + kubectl set image to the ECR/SHA refs

# 5. HARD GATE: prove sandbox NetworkPolicy is actually enforced.
make netpol-gate
```

`make print-images` shows the exact ECR/SHA refs that `deploy` will substitute.
Override the tag with `make images TAG=1a2b3c4` to pin a specific build.

## How image substitution works

The committed manifests in `k8s/` reference `ghcr.io/agrawalx/<name>:dev` (and a
few `:local`) so they also run on the local k3s cluster. `make deploy` keeps the
YAML cloud-agnostic: it `kubectl apply`s the tree, then `kubectl set image` /
`kubectl set env` each workload to the `<registry>/iicpc/<name>:<sha>` ref read
from `terraform output`. Container names were verified against the manifests
(e.g. the `bot-fleet-worker` Deployment's container is `bot-fleet-worker`, not
`bot-fleet`). The eBPF capture image is not a Deployment image — it is injected as
`CAPTURE_IMAGE` on the sandbox-orchestrator via `kubectl set env`.

## Manual steps (residual)

These are deliberately NOT automated.

### 1. Secrets (required before `make deploy`)

`make deploy` aborts if `postgres-secret` is absent. Create every secret
imperatively, sourcing values from AWS Secrets Manager / SSM — never apply the
placeholder `secret.yaml` files (they are `changeme` shapes). Follow
`DEPLOYMENT_EKS.md §7` for the full list. Minimum to boot the data tier + APIs:

```bash
# Pull or generate strong values (example shown generating; prefer Secrets Manager)
export PG_PW=$(openssl rand -hex 24) TS_PW=$(openssl rand -hex 24)
export MINIO_AK=iicpc-$(openssl rand -hex 6) MINIO_SK=$(openssl rand -hex 24)
export KAFKA_ID=$(python3 -c "import uuid,base64;print(base64.urlsafe_b64encode(uuid.uuid4().bytes+uuid.uuid4().bytes[:4]).decode().rstrip('='))")

kubectl create secret generic postgres-secret   -n data --from-literal=password="$PG_PW"
kubectl create secret generic timescaledb-secret -n data --from-literal=password="$TS_PW"
kubectl create secret generic minio-secret       -n data --from-literal=access-key="$MINIO_AK" --from-literal=secret-key="$MINIO_SK"
kubectl create secret generic kafka-secret       -n data --from-literal=cluster-id="$KAFKA_ID"
# ...plus submission-api-secret, spawner-secret, leaderboard-api-secret,
#    score-computer-secret, auth-api secret — see DEPLOYMENT_EKS.md §7.
```

The `spawner-secret`'s `HARBOR_*` fields now point at ECR (the spawner pushes
contestant images there). The IAM permission to push/create lives in the
build-spawner IRSA role, which Terraform already annotated onto the
`build-spawner` ServiceAccount — so the spawner authenticates to ECR by role, not
by a long-lived secret.

### 2. gVisor (only if `RUNTIME_CLASS=gvisor`)

Terraform registers the `gvisor` RuntimeClass *object*, but `runsc` must be
installed on the sandbox nodes' host before a pod can use `handler: runsc`
(`DEPLOYMENT_EKS.md §3.6`). Either bake `runsc` into a custom sandbox AMI or run a
privileged installer DaemonSet on `pool=sandbox` nodes, then set
`RUNTIME_CLASS=gvisor` on the orchestrator:

```bash
kubectl -n sandbox set env deployment/sandbox-orchestrator RUNTIME_CLASS=gvisor
```

If you are not running gVisor, leave `RUNTIME_CLASS=""` (the default; the
orchestrator omits `runtimeClassName`) and rely on the NetworkPolicy + cgroup +
seccomp isolation — in that posture the NetworkPolicy gate below becomes the hard
isolation boundary (MEMORY: gVisor is droppable on EKS).

### 3. Frontend ALB Ingress (the public URL)

`make deploy` applies the `k8s/platform/` manifests, but the public Ingress needs
*your* ACM certificate ARN, so it is not committed with a placeholder that would
fail. Create it from `DEPLOYMENT_EKS.md §3.3` (the `frontend-ingress.yaml` block),
substituting `alb.ingress.kubernetes.io/certificate-arn`. The ALB controller
(installed by Terraform) reconciles it; the public URL is then:

```bash
kubectl get ingress frontend -n platform
```

### 4. NetworkPolicy enforcement gate (do not skip)

On stock EKS the VPC CNI ignores NetworkPolicy objects — `kubectl get netpol`
lists them while nothing is enforced. Terraform turns enforcement ON
(`vpc-cni` addon `enableNetworkPolicy=true`), but you must *prove* it:

```bash
make netpol-gate     # = infra/scripts/netpol-deny-test.sh
```

It launches a throwaway pod in `sandbox` and asserts it CANNOT reach
`postgres.data:5432` (cluster egress denied) while it CAN reach the public
internet (allowlist intact). If the postgres connect succeeds, enforcement is OFF
and the script exits non-zero — stop and fix the CNI before running any contest.
Run it after the data tier is up.

## Cost / smoke note

The `terraform.tfvars.example` defaults to the minimal smoke shape
(`m6i.xlarge` general, `sandbox_min_size=1` so the cpuset hard-floor exists). For
production, bump `general_instance_type` to `m6i.2xlarge` and set
`sandbox_min_size=0` so idle sandbox nodes scale to zero (one algo pod per
sandbox node ⇒ node count = concurrent contestants).

## Teardown

```bash
make destroy
```

It deletes Ingresses first (so the ALB + its ENIs detach cleanly) and then runs
`terraform destroy`. ECR repos use `force_delete = true` so they are removed even
with images present. Note the gp3 StorageClass `reclaimPolicy: Retain` means EBS
volumes from PVCs are NOT auto-deleted — clean those up manually if you want zero
residual cost.
```
