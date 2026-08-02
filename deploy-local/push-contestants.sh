#!/usr/bin/env bash
# Build + push the harness's contestant/sink images to ECR, so the b1–b5 suite
# runs on EKS (HARNESS_ENV=eks). amd64-only: these run on the x86 sandbox pool.
#
#   TAG=<tag> deploy-local/push-contestants.sh
#
# Images (same builds as build-contestants.sh + the b4 sink):
#   iicpc/contestant-matching-engine  correct book, dual listener (QUALIFIES)
#   iicpc/contestant-echo             ACKs everything (DISQUALIFIED)
#   iicpc/stall-sink                  b4's wedged peer
set -euo pipefail
cd "$(dirname "$0")/.."

REGION="${AWS_REGION:-us-east-1}"
ACCOUNT="$(aws sts get-caller-identity --query Account --output text)"
REGISTRY="${ACCOUNT}.dkr.ecr.${REGION}.amazonaws.com"
TAG="${TAG:-$(git rev-parse --short HEAD)}"

aws ecr get-login-password --region "$REGION" | docker login --username AWS --password-stdin "$REGISTRY"

# Repos are outside terraform's service_images list (they are harness fixtures,
# not platform services) — create idempotently.
for repo in iicpc/contestant-matching-engine iicpc/contestant-echo iicpc/stall-sink; do
  aws ecr describe-repositories --repository-names "$repo" --region "$REGION" >/dev/null 2>&1 \
    || aws ecr create-repository --repository-name "$repo" --region "$REGION" >/dev/null
done

echo "=== push iicpc/contestant-matching-engine:$TAG ==="
docker buildx build --platform linux/amd64 \
  -f deploy-local/contestant-matching-engine.Dockerfile \
  -t "$REGISTRY/iicpc/contestant-matching-engine:$TAG" --push e2e/contestant-matching-engine

echo "=== push iicpc/contestant-echo:$TAG ==="
docker buildx build --platform linux/amd64 \
  -f services/bot-fleet/contestant-echo.Dockerfile \
  -t "$REGISTRY/iicpc/contestant-echo:$TAG" --push .

echo "=== push iicpc/stall-sink:$TAG ==="
docker buildx build --platform linux/amd64 \
  -f deploy-local/stall-sink/Dockerfile \
  -t "$REGISTRY/iicpc/stall-sink:$TAG" --push deploy-local/stall-sink

echo
echo "############ CONTESTANT IMAGES PUSHED ($TAG) ############"
echo ">> run the suite: HARNESS_ENV=eks CONTESTANT_TAG=$TAG deploy-local/b2-two-pass.sh"
