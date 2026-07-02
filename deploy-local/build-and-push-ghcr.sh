#!/usr/bin/env bash
# Build every IICPC platform image from the CURRENT working tree and push it to
# the public GHCR namespace used by the clone-and-go local deploy (deploy-local/up.sh).
#
#   ghcr.io/agrawalx/<service>:demo
#
# Usage:
#   docker login ghcr.io -u agrawalx          # PAT with write:packages (once)
#   deploy-local/build-and-push-ghcr.sh        # build + push all
#   PHASE=build deploy-local/build-and-push-ghcr.sh   # build only (no login needed)
#   PHASE=push  deploy-local/build-and-push-ghcr.sh   # push already-built images
#
# After the first push, make each new package PUBLIC at
#   https://github.com/users/agrawalx/packages  (package -> Settings -> Change visibility)
# so a fresh clone can pull without credentials.
set -euo pipefail
cd "$(dirname "$0")/.."

REPO="${REPO:-ghcr.io/agrawalx}"
TAG="${TAG:-demo}"
PHASE="${PHASE:-all}"   # all | build | push

# image short-name -> Dockerfile (repo-root build context unless noted)
GO_SVCS=(
  "submission-api:services/submission-api/Dockerfile"
  "spawner:services/build-worker/Dockerfile"
  "sandbox-orchestrator:services/sandbox-orchestrator/Dockerfile"
  "bot-fleet-controller:services/bot-fleet-controller/Dockerfile"
  "correctness-validator:services/correctness-validator/Dockerfile"
  "auth-api:services/auth-api/Dockerfile"
  "score-computer:services/score-computer/Dockerfile"
  "leaderboard-api:services/leaderboard-api/Dockerfile"
)
RUST_SVCS=(
  "bot-fleet:services/bot-fleet/Dockerfile"
  "telemetry-ingester:services/telemetry-ingester/Dockerfile"
  "ebpf-latency:services/ebpf-latency/Dockerfile"
)

ALL_IMAGES=()

build_one() {
  local name="$1" dockerfile="$2" img="${REPO}/${1}:${TAG}"
  echo "=== build ${img} ==="
  docker build -f "$dockerfile" -t "$img" .
  ALL_IMAGES+=("$img")
}

build_frontend() {
  local img="${REPO}/frontend:${TAG}"
  echo "=== build ${img} (auth is hardcoded off; no Google build-args needed) ==="
  docker build -f frontend/Dockerfile -t "$img" frontend
  ALL_IMAGES+=("$img")
}

collect_names() {
  ALL_IMAGES=()
  for s in "${GO_SVCS[@]}" "${RUST_SVCS[@]}"; do ALL_IMAGES+=("${REPO}/${s%%:*}:${TAG}"); done
  ALL_IMAGES+=("${REPO}/frontend:${TAG}")
}

if [ "$PHASE" = "all" ] || [ "$PHASE" = "build" ]; then
  for s in "${GO_SVCS[@]}";   do build_one "${s%%:*}" "${s#*:}"; done
  for s in "${RUST_SVCS[@]}"; do build_one "${s%%:*}" "${s#*:}"; done
  build_frontend
  echo "ALL IMAGES BUILT (${#ALL_IMAGES[@]})"
fi

if [ "$PHASE" = "all" ] || [ "$PHASE" = "push" ]; then
  [ "$PHASE" = "push" ] && collect_names
  for img in "${ALL_IMAGES[@]}"; do
    echo "=== push ${img} ==="
    docker push "$img"
  done
  echo "ALL IMAGES PUSHED to ${REPO} (tag ${TAG})"
  echo ">> Now make each package PUBLIC: https://github.com/users/agrawalx/packages"
fi
