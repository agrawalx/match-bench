# infra/Makefile — end-to-end EKS bring-up driver.
#
# Problem: the bring-up is Terraform (cluster/ECR/IRSA/addons) + a docker build &
# push of 12 images by git SHA + a dependency-ordered `kubectl apply` of k8s/ with
# the image fields substituted from ghcr.io/...:dev to the ECR/SHA refs. Doing
# that by hand is error-prone (wrong order deadlocks Kafka; a stale tag scores
# contestants against different binaries). Decision: one Makefile that reads the
# ECR URLs and registry from `terraform output`, builds/pushes by SHA, and applies
# k8s/ in the order from DEPLOYMENT_EKS.md §6/§12. Why read from terraform output:
# no value is hard-coded twice; the Makefile and Terraform agree by construction.
#
# Usage:  make init plan apply kubeconfig ecr-login images deploy
#         make destroy   (tears the cluster down)

# /bin/bash, not `/usr/bin/env bash` — GNU make treats SHELL as a literal path and
# does not word-split it, so the env form fails with "/usr/bin/env bash: No such
# file or directory" on some hosts.
SHELL := /bin/bash
.ONESHELL:
.SHELLFLAGS := -eu -o pipefail -c

# Repo root is one level up from infra/.
REPO_ROOT := $(abspath $(dir $(abspath $(lastword $(MAKEFILE_LIST))))/..)
TF_DIR    := $(REPO_ROOT)/infra/terraform
K8S       := $(REPO_ROOT)/k8s

# Resolved lazily (= not :=) so they only run terraform when a target needs them.
REGION      = $(shell cd $(TF_DIR) && terraform output -raw region)
CLUSTER     = $(shell cd $(TF_DIR) && terraform output -raw cluster_name)
REGISTRY    = $(shell cd $(TF_DIR) && terraform output -raw ecr_registry)
SPAWNER_ARN = $(shell cd $(TF_DIR) && terraform output -json irsa_role_arns | python3 -c "import sys,json;print(json.load(sys.stdin)['build_spawner'])")
TAG        ?= $(shell git -C $(REPO_ROOT) rev-parse --short HEAD)

# The 12 images as "imageName:buildContextDir:dockerfile". The first 11 build from
# the repo root (Go/Rust workspace); frontend builds from frontend/. Image name
# 'spawner' comes from services/build-worker (DEPLOYMENT §1.2).
IMAGES := \
  auth-api:.:services/auth-api/Dockerfile \
  submission-api:.:services/submission-api/Dockerfile \
  leaderboard-api:.:services/leaderboard-api/Dockerfile \
  spawner:.:services/build-worker/Dockerfile \
  sandbox-orchestrator:.:services/sandbox-orchestrator/Dockerfile \
  bot-fleet-controller:.:services/bot-fleet-controller/Dockerfile \
  bot-fleet:.:services/bot-fleet/Dockerfile \
  ebpf-latency:.:services/ebpf-latency/Dockerfile \
  telemetry-ingester:.:services/telemetry-ingester/Dockerfile \
  correctness-validator:.:services/correctness-validator/Dockerfile \
  score-computer:.:services/score-computer/Dockerfile \
  frontend:frontend:frontend/Dockerfile

.PHONY: help init plan apply kubeconfig ecr-login images deploy destroy print-images netpol-gate

help:
	@echo "Targets:"
	@echo "  init        terraform init"
	@echo "  plan        terraform plan"
	@echo "  apply       terraform apply (cluster, ECR, IRSA, addons)"
	@echo "  kubeconfig  point kubectl at the cluster"
	@echo "  ecr-login   docker login to ECR"
	@echo "  images      build + push all 12 images to ECR tagged by git SHA (TAG=$(TAG))"
	@echo "  deploy      kubectl apply k8s/ in dependency order with image substitution"
	@echo "  netpol-gate run the sandbox isolation deny-test (HARD GATE)"
	@echo "  destroy     terraform destroy"

# ---------------------------------------------------------------------------
# Terraform lifecycle
# ---------------------------------------------------------------------------
init:
	cd $(TF_DIR) && terraform init

plan:
	cd $(TF_DIR) && terraform plan

apply:
	cd $(TF_DIR) && terraform apply

destroy:
	# Scale sandbox/benchmark workloads down first so the ALB + ENIs detach cleanly,
	# then destroy. Ignore errors if the cluster is already gone.
	-kubectl delete ingress --all -A --wait=true 2>/dev/null || true
	cd $(TF_DIR) && terraform destroy

# ---------------------------------------------------------------------------
# Cluster access + registry auth
# ---------------------------------------------------------------------------
kubeconfig:
	aws eks update-kubeconfig --name "$(CLUSTER)" --region "$(REGION)"
	kubectl get nodes -L pool,role

ecr-login:
	aws ecr get-login-password --region "$(REGION)" \
	  | docker login --username AWS --password-stdin "$(REGISTRY)"

# ---------------------------------------------------------------------------
# Build + push all 12 images by git SHA
# ---------------------------------------------------------------------------
# Each ECR repo is iicpc/<name> (created by Terraform). Build for linux/amd64 —
# the ebpf-latency image bakes a bpfel-unknown-none BPF object and must be x86_64
# (DEPLOYMENT §1.2). Tag by SHA so a tag never moves under a running contest.
images: ecr-login
	@echo ">> building + pushing 12 images @ $(TAG) to $(REGISTRY)/iicpc/*"
	for spec in $(IMAGES); do
	  name="$${spec%%:*}"; rest="$${spec#*:}"; ctx="$${rest%%:*}"; dockerfile="$${rest#*:}"
	  ref="$(REGISTRY)/iicpc/$$name:$(TAG)"
	  echo "   -> $$ref  (context=$$ctx, dockerfile=$$dockerfile)"
	  docker build --platform linux/amd64 -f "$(REPO_ROOT)/$$dockerfile" -t "$$ref" "$(REPO_ROOT)/$$ctx"
	  docker push "$$ref"
	done
	@echo ">> done. image tag = $(TAG)"

print-images:
	@for spec in $(IMAGES); do name="$${spec%%:*}"; echo "$(REGISTRY)/iicpc/$$name:$(TAG)"; done

# ---------------------------------------------------------------------------
# Deploy k8s/ in dependency order with image substitution
# ---------------------------------------------------------------------------
# Order mirrors DEPLOYMENT_EKS.md §6/§12: namespaces -> (CNI already on via TF)
# -> storageclass (TF) -> secrets (MANUAL, see README) -> data tier + topic init
# -> observability -> platform APIs -> build -> sandbox -> benchmark -> frontend.
#
# Image substitution: the committed manifests say ghcr.io/agrawalx/<name>:dev (and
# a few :local). We `kubectl apply` the tree, then `kubectl set image` each
# workload to the ECR/SHA ref — exactly the local guide's pattern, so the in-tree
# YAML stays cloud-agnostic. CAPTURE_IMAGE (ebpf-latency) is injected via the
# orchestrator env, not a Deployment image, so it is set with `kubectl set env`.
deploy:
	@command -v kubectl >/dev/null
	REG="$(REGISTRY)"; T="$(TAG)"
	echo ">> [1] namespaces (carry the name: <ns> labels the NetworkPolicies select on)"
	kubectl apply -f $(K8S)/data/namespace.yaml -f $(K8S)/platform/namespace.yaml \
	  -f $(K8S)/build/namespace.yaml -f $(K8S)/sandbox/namespace.yaml \
	  -f $(K8S)/benchmark/namespace.yaml -f $(K8S)/observability/namespace.yaml

	echo ">> [2] network policies (CNI enforcement is enabled by Terraform vpc-cni addon)"
	kubectl apply -f $(K8S)/data/network-policy.yaml -f $(K8S)/platform/network-policy.yaml \
	  -f $(K8S)/build/network-policy.yaml -f $(K8S)/sandbox/network-policy.yaml \
	  -f $(K8S)/benchmark/network-policy.yaml -f $(K8S)/observability/network-policy.yaml

	echo ">> [3] SECRETS are a MANUAL step — see infra/README.md. Aborting if data secrets absent."
	kubectl -n data get secret postgres-secret >/dev/null 2>&1 || { \
	  echo "   ERROR: data-tier secrets not found. Create them (README 'Manual steps') then re-run."; exit 1; }

	echo ">> [4] data tier (Services then StatefulSets) + wait for Kafka quorum + topic init"
	kubectl apply -R -f $(K8S)/data/
	kubectl -n data rollout status statefulset/postgres --timeout=300s || true
	kubectl -n data wait --for=condition=Ready pod -l app=kafka --timeout=420s || true
	kubectl -n data wait --for=condition=complete job/kafka-topic-init --timeout=300s 2>/dev/null || true

	echo ">> [5] observability"
	kubectl apply -R -f $(K8S)/observability/

	echo ">> [6] platform APIs (auth-api, submission-api, leaderboard-api, frontend manifests)"
	kubectl apply -R -f $(K8S)/platform/
	kubectl -n platform set image deployment/auth-api        auth-api=$$REG/iicpc/auth-api:$$T 2>/dev/null || true
	kubectl -n platform set image deployment/submission-api  submission-api=$$REG/iicpc/submission-api:$$T 2>/dev/null || true
	kubectl -n platform set image deployment/leaderboard-api leaderboard-api=$$REG/iicpc/leaderboard-api:$$T 2>/dev/null || true
	kubectl -n platform set image deployment/frontend        frontend=$$REG/iicpc/frontend:$$T 2>/dev/null || true

	echo ">> [7] build (substitute spawner IRSA role ARN into the SA, then spawner image)"
	kubectl apply -R -f $(K8S)/build/
	kubectl -n build annotate serviceaccount build-spawner \
	  eks.amazonaws.com/role-arn="$(SPAWNER_ARN)" --overwrite
	kubectl -n build set image deployment/spawner spawner=$$REG/iicpc/spawner:$$T 2>/dev/null || true

	echo ">> [8] sandbox (RBAC, netpol, orchestrator) + inject CAPTURE_IMAGE = ebpf-latency ref"
	kubectl apply -R -f $(K8S)/sandbox/
	kubectl -n sandbox set image deployment/sandbox-orchestrator sandbox-orchestrator=$$REG/iicpc/sandbox-orchestrator:$$T 2>/dev/null || true
	kubectl -n sandbox set env deployment/sandbox-orchestrator CAPTURE_IMAGE=$$REG/iicpc/ebpf-latency:$$T 2>/dev/null || true

	echo ">> [9] benchmark (netpol, controller, worker+ScaledObject, ingester, validator, score-computer)"
	kubectl apply -R -f $(K8S)/benchmark/
	kubectl -n benchmark set image deployment/bot-fleet-controller bot-fleet-controller=$$REG/iicpc/bot-fleet-controller:$$T 2>/dev/null || true
	kubectl -n benchmark set image deployment/bot-fleet-worker     bot-fleet-worker=$$REG/iicpc/bot-fleet:$$T 2>/dev/null || true
	kubectl -n benchmark set image deployment/telemetry-ingester   telemetry-ingester=$$REG/iicpc/telemetry-ingester:$$T 2>/dev/null || true
	kubectl -n benchmark set image deployment/correctness-validator correctness-validator=$$REG/iicpc/correctness-validator:$$T 2>/dev/null || true
	kubectl -n benchmark set image deployment/score-computer        score-computer=$$REG/iicpc/score-computer:$$T 2>/dev/null || true

	echo ">> deploy applied. Frontend ALB Ingress + secrets/CAPTURE substitution may need review (README)."
	echo ">> RUN THE GATE: make netpol-gate"

netpol-gate:
	bash $(REPO_ROOT)/infra/scripts/netpol-deny-test.sh
