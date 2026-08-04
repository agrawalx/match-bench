#!/usr/bin/env bash
# production/01-cluster.sh — provision the cluster and point kubectl at it.
#
# Billing starts here (~$2/hour for the baseline shape).
#
# The `-var enable_spawner_irsa=false` override is REQUIRED on a fresh cluster
# and is not optional tidiness: infra/terraform-v2/tfvars/contest.tfvars sets it
# true (correct for every later apply), but kubernetes_annotations PATCHES an
# existing object and has no depends_on, and the build-spawner ServiceAccount is
# created by the platform manifests — which are applied two phases later. On a
# fresh cluster the first apply would fail with "ServiceAccount not found".
# 04-irsa.sh runs the second apply, without the override.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/production/lib/log.sh"
source "$ROOT/production/lib/k8s.sh"
source "$ROOT/production/lib/aws.sh"
source "$ROOT/production/config/production.env"
export AWS_PROFILE AWS_REGION

main() {
  phase "Cluster"
  need terraform; need aws; need kubectl

  step "terraform init"
  terraform -chdir="$ROOT/$TF_DIR" init -backend-config=backend.hcl -input=false >/dev/null \
    || die "terraform init failed"
  ok "backend initialised"

  step "terraform apply (irsa deferred to 04)"
  info "this takes ~20 minutes and starts billing"
  terraform -chdir="$ROOT/$TF_DIR" apply \
    -var-file="$TFVARS" \
    -var enable_spawner_irsa=false \
    -input=false -auto-approve \
    || die "terraform apply failed"
  ok "cluster provisioned"

  step "kubeconfig"
  aws eks update-kubeconfig --name "$CLUSTER_NAME" --region "$AWS_REGION" >/dev/null \
    || die "update-kubeconfig failed for $CLUSTER_NAME"
  require_context "$CLUSTER_NAME"

  step "node shape"
  # Asserted here rather than trusted: a node group that silently came up short
  # produces failures three phases later that look like application bugs.
  local pool want have total=0
  for pool in general kafka sandbox botworker; do
    case "$pool" in
      general)   want="$EXPECT_GENERAL_NODES" ;;
      kafka)     want="$EXPECT_KAFKA_NODES" ;;
      sandbox)   want="$EXPECT_SANDBOX_NODES" ;;
      botworker) want="$EXPECT_BOTWORKER_NODES" ;;
    esac
    have="$(nodes_in_pool "$pool")"
    total=$(( total + have ))
    if [ "$have" -eq "$want" ]; then
      ok "$pool: $have node(s)"
    else
      fail "$pool: $have node(s), expected $want"
    fi
  done
  info "total nodes: $total"

  step "node readiness"
  local notready
  notready="$(kubectl get nodes --no-headers 2>/dev/null | awk '$2 != "Ready" {print $1}' || true)"
  if [ -n "$notready" ]; then
    fail "nodes not Ready: $(echo "$notready" | tr '\n' ' ')"
  else
    ok "all nodes Ready"
  fi

  step "terraform-managed in-cluster addons"
  # These come from terraform, not from the overlay. The gp3 StorageClass in
  # particular is a hard dependency of the next phase: every data PVC binds
  # through it, so a missing class leaves Kafka/Postgres/MinIO Pending forever.
  if kubectl get storageclass gp3 >/dev/null 2>&1; then
    ok "gp3 StorageClass"
  else
    fail "gp3 StorageClass missing — the data-plane PVCs cannot bind without it"
  fi

  finish "Cluster"
}

main "$@"
