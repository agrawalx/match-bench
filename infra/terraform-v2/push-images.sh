#!/usr/bin/env bash
# infra/terraform-v2/push-images.sh — build + push every service image to ECR
# and stamp the immutable tag into the eks-contest overlay.
#
#   ./push-images.sh [tag]        default tag: git short sha (immutable repos:
#                                 re-pushing an existing tag FAILS, by design —
#                                 new code means a new tag)
#
# bot-fleet is the ONE dual-arch build (linux/amd64 + linux/arm64 manifest
# list): local x86 k3s and the Graviton botworker pool pull the same tag and
# each gets its own arch. Everything else runs on x86 pools only and gets a
# plain amd64 build. Requires: docker buildx (with binfmt/QEMU for the arm
# cross-build: `docker run --privileged --rm tonistiigi/binfmt --install arm64`
# once per machine), AWS credentials, and the ECR repos from terraform-v2.
set -euo pipefail
cd "$(dirname "$0")/../.."

REGION="${REGION:-us-east-1}"
ACCOUNT="$(aws sts get-caller-identity --query Account --output text)"
REGISTRY="${ACCOUNT}.dkr.ecr.${REGION}.amazonaws.com"
TAG="${1:-$(git rev-parse --short HEAD)}"
OVERLAY="overlays/eks-contest/kustomization.yaml"

echo ">> registry $REGISTRY  tag $TAG"
aws ecr get-login-password --region "$REGION" | docker login --username AWS --password-stdin "$REGISTRY"

# Reuse the same Dockerfile->image mapping as deploy-local/build-dev-images.sh.
# Format: <dockerfile> <repo> [context]
x86_builds() {
  cat <<EOF
services/submission-api/Dockerfile        iicpc/submission-api
services/build-worker/Dockerfile          iicpc/spawner
services/sandbox-orchestrator/Dockerfile  iicpc/sandbox-orchestrator
services/bot-fleet-controller/Dockerfile  iicpc/bot-fleet-controller
services/correctness-validator/Dockerfile iicpc/correctness-validator
services/auth-api/Dockerfile              iicpc/auth-api
services/score-computer/Dockerfile        iicpc/score-computer
services/leaderboard-api/Dockerfile       iicpc/leaderboard-api
frontend/Dockerfile                       iicpc/frontend frontend
services/telemetry-ingester/Dockerfile    iicpc/telemetry-ingester
services/ebpf-latency/Dockerfile          iicpc/ebpf-latency
EOF
}

while read -r df repo ctx; do
  [ -n "$df" ] || continue
  echo "=== amd64 build+push $repo:$TAG  ($(date +%H:%M:%S)) ==="
  docker buildx build --platform linux/amd64 \
    -f "$df" -t "$REGISTRY/$repo:$TAG" --push "${ctx:-.}"
done < <(x86_builds)

echo "=== MULTI-ARCH build+push iicpc/bot-fleet:$TAG (amd64+arm64) ==="
docker buildx build --platform linux/amd64,linux/arm64 \
  -f services/bot-fleet/Dockerfile -t "$REGISTRY/iicpc/bot-fleet:$TAG" --push .

echo ">> stamping tag $TAG into $OVERLAY"
sed -i "s/newTag: .*/newTag: \"$TAG\"/" "$OVERLAY"

echo
echo "############ PUSHED $TAG ############"
echo ">> commit the overlay stamp, then: kubectl apply -k overlays/eks-contest"
