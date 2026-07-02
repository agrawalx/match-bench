#!/usr/bin/env bash
# Build all platform images locally, tagged ghcr.io/agrawalx/<svc>:demo to match
# the committed manifests. Either `docker push` them to GHCR, or (for a local
# k3s) `docker save | k3s ctr images import` — IfNotPresent then finds them.
set -euo pipefail
cd "$(dirname "$0")/.."

bld() { echo "=== build $2 ==="; docker build -f "$1" -t "$2" "${3:-.}" ; }

# Go services (repo-root context)
bld services/submission-api/Dockerfile        ghcr.io/agrawalx/submission-api:demo
bld services/build-worker/Dockerfile          ghcr.io/agrawalx/spawner:demo
bld services/sandbox-orchestrator/Dockerfile  ghcr.io/agrawalx/sandbox-orchestrator:demo
bld services/bot-fleet-controller/Dockerfile  ghcr.io/agrawalx/bot-fleet-controller:demo
bld services/correctness-validator/Dockerfile ghcr.io/agrawalx/correctness-validator:demo
bld services/auth-api/Dockerfile              ghcr.io/agrawalx/auth-api:demo
bld services/score-computer/Dockerfile        ghcr.io/agrawalx/score-computer:demo
bld services/leaderboard-api/Dockerfile       ghcr.io/agrawalx/leaderboard-api:demo

# Rust services (repo-root context; slower)
bld services/bot-fleet/Dockerfile             ghcr.io/agrawalx/bot-fleet:demo
bld services/telemetry-ingester/Dockerfile    ghcr.io/agrawalx/telemetry-ingester:demo
bld services/ebpf-latency/Dockerfile          ghcr.io/agrawalx/ebpf-latency:demo

# Frontend (frontend/ context). Auth is disabled platform-wide, so no Google
# OAuth build-args are baked in anymore.
bld frontend/Dockerfile                       ghcr.io/agrawalx/frontend:demo frontend

echo "ALL IMAGES BUILT"
docker images --format '{{.Repository}}:{{.Tag}}\t{{.Size}}' | grep -E 'agrawalx' | sort
