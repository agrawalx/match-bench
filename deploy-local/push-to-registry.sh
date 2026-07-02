#!/usr/bin/env bash
# Retag fresh images under a unique tag and push to the in-cluster registry.
# Avoids stale :dev/:local collisions in containerd and is GC-proof (re-pullable).
# Push via localhost:5000 (docker treats localhost as insecure); the node pulls
# the same repo via the 192.168.1.11:5000 registries.yaml mirror.
set -euo pipefail
TAG="${TAG:-fix1}"
declare -A M=(
  [ghcr.io/agrawalx/submission-api:dev]=submission-api
  [ghcr.io/agrawalx/spawner:dev]=spawner
  [ghcr.io/agrawalx/sandbox-orchestrator:dev]=sandbox-orchestrator
  [ghcr.io/agrawalx/bot-fleet-controller:dev]=bot-fleet-controller
  [ghcr.io/agrawalx/bot-fleet:dev]=bot-fleet
  [ghcr.io/agrawalx/correctness-validator:dev]=correctness-validator
  [ghcr.io/agrawalx/auth-api:dev]=auth-api
  [ghcr.io/agrawalx/telemetry-ingester:dev]=telemetry-ingester
  [score-computer:local]=score-computer
  [leaderboard-api:local]=leaderboard-api
  [frontend:local]=frontend
  [iicpc/ebpf-latency:dev]=ebpf-latency
)
for src in "${!M[@]}"; do
  dst="localhost:5000/iicpc/${M[$src]}:${TAG}"
  echo "=== push $dst ==="
  docker tag "$src" "$dst"
  docker push "$dst"
done
echo "ALL PUSHED (tag $TAG)"
