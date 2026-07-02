#!/usr/bin/env bash
# Save all locally-built platform images into one tar for k3s import.
set -euo pipefail
cd "$(dirname "$0")"
IMAGES=(
  ghcr.io/agrawalx/submission-api:dev
  ghcr.io/agrawalx/spawner:dev
  ghcr.io/agrawalx/sandbox-orchestrator:dev
  ghcr.io/agrawalx/bot-fleet-controller:dev
  ghcr.io/agrawalx/correctness-validator:dev
  ghcr.io/agrawalx/auth-api:dev
  ghcr.io/agrawalx/bot-fleet:dev
  ghcr.io/agrawalx/telemetry-ingester:dev
  score-computer:local
  leaderboard-api:local
  iicpc/ebpf-latency:dev
  frontend:local
)
echo ">> saving ${#IMAGES[@]} images to images.tar ..."
docker save "${IMAGES[@]}" -o images.tar
echo ">> done: $(du -h images.tar | cut -f1)"
