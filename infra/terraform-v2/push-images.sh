#!/usr/bin/env bash
# infra/terraform-v2/push-images.sh — build + push every service image to ECR
# and stamp the immutable tag into the eks-contest overlay.
#
#   ./push-images.sh [tag]        default tag: git short sha (immutable repos:
#                                 re-pushing an existing tag FAILS, by design —
#                                 new code means a new tag)
#
# bot-fleet is the ONE dual-arch build (linux/amd64 + linux/arm64 manifest
# list) and REQUIRES a docker-container buildx builder — the default docker
# driver cannot do multi-platform (bitten 2026-08-02):
#   docker buildx create --name multiarch --driver docker-container
# plus binfmt/QEMU once per machine:
#   docker run --privileged --rm tonistiigi/binfmt --install arm64
# Everything else is x86-only and uses plain docker build+push (reuses the
# daemon layer cache). Includes the measurement FIXTURES (drain-sink,
# stall-sink) — platform tooling, not contestants; real contestant images are
# produced ONLY by the build pipeline from submitted zips (decided
# 2026-08-02; push-contestants.sh deleted for that reason).
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
deploy-local/drain-sink/Dockerfile        iicpc/drain-sink deploy-local/drain-sink
deploy-local/stall-sink/Dockerfile        iicpc/stall-sink deploy-local/stall-sink
EOF
}

while read -r df repo ctx; do
  [ -n "$df" ] || continue
  echo "=== amd64 build+push $repo:$TAG  ($(date +%H:%M:%S)) ==="
  docker build -f "$df" -t "$REGISTRY/$repo:$TAG" "${ctx:-.}"
  docker push "$REGISTRY/$repo:$TAG"
done < <(x86_builds)

echo "=== MULTI-ARCH build+push iicpc/bot-fleet:$TAG (amd64+arm64) ==="
docker buildx build --builder multiarch --platform linux/amd64,linux/arm64 \
  -f services/bot-fleet/Dockerfile -t "$REGISTRY/iicpc/bot-fleet:$TAG" --push .

echo ">> stamping tag $TAG into $OVERLAY"
sed -i "s/newTag: .*/newTag: \"$TAG\"/" "$OVERLAY"

echo
echo "############ PUSHED $TAG ############"
echo ">> commit the overlay stamp, then: kubectl apply -k overlays/eks-contest"
