#!/usr/bin/env bash
# Build + push all 13 service images to ECR for linux/amd64, tagged by git SHA.
# Mirrors the infra/Makefile IMAGES list (make is unusable on this host — the
# SHELL := /usr/bin/env bash line doesn't parse). Run from repo root.
set -uo pipefail

REGISTRY="885232248981.dkr.ecr.us-east-1.amazonaws.com"
TAG="$(git rev-parse --short HEAD)"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

IMAGES=(
  "auth-api:.:services/auth-api/Dockerfile"
  "submission-api:.:services/submission-api/Dockerfile"
  "leaderboard-api:.:services/leaderboard-api/Dockerfile"
  "spawner:.:services/build-worker/Dockerfile"
  "sandbox-orchestrator:.:services/sandbox-orchestrator/Dockerfile"
  "bot-fleet-controller:.:services/bot-fleet-controller/Dockerfile"
  "bot-fleet:.:services/bot-fleet/Dockerfile"
  "ebpf-latency:.:services/ebpf-latency/Dockerfile"
  "telemetry-ingester:.:services/telemetry-ingester/Dockerfile"
  "correctness-validator:.:services/correctness-validator/Dockerfile"
  "score-computer:.:services/score-computer/Dockerfile"
  "frontend:frontend:frontend/Dockerfile"
)

ok=(); fail=()
for spec in "${IMAGES[@]}"; do
  name="${spec%%:*}"; rest="${spec#*:}"; ctx="${rest%%:*}"; df="${rest#*:}"
  ref="$REGISTRY/iicpc/$name:$TAG"
  echo ">>> [$(date +%H:%M:%S)] building $name -> $ref (ctx=$ctx df=$df)"
  if docker build --platform linux/amd64 -f "$ROOT/$df" -t "$ref" "$ROOT/$ctx" \
       && docker push "$ref"; then
    echo ">>> [$(date +%H:%M:%S)] OK $name"
    ok+=("$name")
  else
    echo ">>> [$(date +%H:%M:%S)] FAIL $name"
    fail+=("$name")
  fi
done

echo "================ SUMMARY (tag=$TAG) ================"
echo "OK   (${#ok[@]}): ${ok[*]}"
echo "FAIL (${#fail[@]}): ${fail[*]}"
[ ${#fail[@]} -eq 0 ] && echo "ALL_IMAGES_PUSHED" || echo "SOME_IMAGES_FAILED"
