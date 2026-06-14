#!/usr/bin/env bash
# Build + push every image the e2e needs to ECR, tagged with the git SHA.
# Run from repo root after the cluster is up.
#
# The 12 platform images (bot-fleet, ebpf-latency, frontend, submission-api,
# leaderboard-api, sandbox-orchestrator, bot-fleet-controller, correctness-validator,
# score-computer, telemetry-ingester, spawner/build-worker, auth-api) build via the
# infra Makefile `images` target (the single source of truth for build contexts +
# Dockerfiles). The echo contestant has its own Dockerfile and is built separately.
#
# The other two contestants ship as SUBMISSION ZIPS (built in-cluster by the
# build-worker when you upload them in the UI), not images:
#   e2e/contestant-matching-engine.zip  — correct order book -> QUALIFIED (the reference)
#   e2e/contestant-fix-acker.zip        — std FIX acker (throughput, disqualifies)
set -euo pipefail
. "$(dirname "$0")/lib.sh"
cd "$REPO_ROOT"
TAG="${TAG:-$(git rev-parse --short HEAD)}"
ecr_login

echo ">> building + pushing the 12 platform images via Makefile @ $TAG"
make -C infra images TAG="$TAG"

echo ">> contestant-echo (tokio FIX echo, ACKs every order on :9898) @ $TAG"
docker build --platform linux/amd64 -f services/bot-fleet/contestant-echo.Dockerfile \
  -t "$REG/iicpc/contestant-echo:$TAG" . >/dev/null
docker push "$REG/iicpc/contestant-echo:$TAG" >/dev/null

echo ">> all images pushed at tag $TAG"
echo "   export TAG=$TAG  before running 02-bootstrap.sh"
