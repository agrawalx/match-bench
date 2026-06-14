#!/usr/bin/env bash
# Bring up the FULL e2e platform: core (data tier incl. 2-broker Kafka, platform APIs,
# benchmark tier, sandbox orchestrator) + eBPF capture + build-worker (zip->image) +
# frontend (auth removed), with the validated sizing. Nothing stubbed.
# Run from repo root after 01-images.sh. Requires TAG (the image tag pushed).
#
# Does NOT seed scenarios — run 04-scenarios.sh after this.
set -euo pipefail
. "$(dirname "$0")/lib.sh"
cd "$REPO_ROOT"
: "${TAG:?set TAG to the image tag from 01-images.sh}"
ACCT="$(aws sts get-caller-identity --query Account --output text)"
ROLE_ARN="arn:aws:iam::${ACCT}:role/iicpc-prod-build-spawner"

# ── 1. core platform: data tier (2-broker Kafka RF=1 + topics, Postgres, TimescaleDB,
#       Redis, MinIO), platform APIs, benchmark tier, sandbox orchestrator. eks-up.sh
#       does the 2-broker Kafka sed + RF=1 topic creation + single-tag image set. ──
echo ">> [1/6] core platform (2-broker kafka, data, platform, benchmark, sandbox) @ $TAG"
TAG="$TAG" bash deploy-bench/eks-up.sh

# (optional) higher-throughput gp3 for Kafka — only needed for >150k/s stress runs;
# the default 125 MB/s gp3 is fine for the e2e's <=100k/s scenarios. Best-effort.
kubectl apply -f deploy-bench/kafka-gp3-throughput.yaml >/dev/null 2>&1 || true

# ── 2. eBPF capture ON + contestant sizing (ALGO_MEMORY=4Gi: a matching engine holds
#       the resting order book in RAM; 1Gi OOMs it). Orchestrator spawns the capture
#       Job alongside each contestant slot. ──
echo ">> [2/6] eBPF capture ON + contestant ALGO_CPU=2 / ALGO_MEMORY=4Gi"
kubectl -n sandbox set env deploy/sandbox-orchestrator \
  CAPTURE_ENABLED=true \
  CAPTURE_IMAGE="$REG/iicpc/ebpf-latency:$TAG" \
  SANDBOX_NODE_POOL=sandbox \
  ALGO_CPU=2 ALGO_MEMORY=4Gi

# CAPTURE FIDELITY (REQUIRED on EKS — see README "Capture fidelity"). EKS nodes use
# jumbo MTU 9001 with offloads on; the capture's CAPTURE_CAP is 1536B, so GSO/GRO
# super-frames get truncated -> FIX framing corrupted -> requests never match
# responses -> ~98% of latency samples lost. Two fixes pin frames to <=1500B:
#   (a) the bot-fleet worker's net-tune initContainer sets its eth0 MTU 1500 + GSO off
#       (already in k8s/benchmark/bot-fleet/deployment.yaml, applied by eks-up), and
#   (b) this DaemonSet disables GRO on the sandbox nodes' host interfaces so cross-node
#       request segments aren't re-coalesced before the (generic-mode) XDP capture.
kubectl apply -f k8s/sandbox/gro-disable-daemonset.yaml
kubectl -n sandbox rollout status ds/gro-disable --timeout=90s || true

# ── 3. load generator: telemetry ON, bounded in-flight (in the manifest), KEDA off so
#       the worker count is deterministic (1 per botworker node), 1000 tasks/worker. ──
echo ">> [3/6] worker: telemetry ON, KEDA off, 1 replica"
kubectl -n benchmark delete scaledobject bot-fleet-worker --ignore-not-found >/dev/null 2>&1 || true
kubectl -n benchmark set env deploy/bot-fleet-worker BOT_DISABLE_TELEMETRY- MAX_BOTS_PER_WORKER=1000 >/dev/null 2>&1 || true
kubectl -n benchmark scale deploy/bot-fleet-worker --replicas=1 >/dev/null
# one ingester replica is enough for one worker; rollup merges its partials.
kubectl -n benchmark scale deploy/telemetry-ingester --replicas=1 >/dev/null 2>&1 || true

# ── 4. correctness-validator sized for the default scenarios (<=100k/s, a few M
#       orders). 8Gi/300s fits comfortably; bump for 500k stress runs. ──
echo ">> [4/6] validator 8Gi / 300s timeout"
kubectl -n benchmark set resources deploy/correctness-validator \
  --limits=memory=8Gi,cpu=2 --requests=memory=1Gi,cpu=500m
kubectl -n benchmark set env deploy/correctness-validator \
  VALIDATION_TIMEOUT_MS=300000 SETTLE_DELAY_MS=15000

# ── 5. build-worker (zip -> image via Kaniko -> ECR via IRSA) for the frontend
#       submission flow. ──
echo ">> [5/6] build-worker (zip -> ECR)"
kubectl apply -f k8s/build/namespace.yaml -f k8s/build/serviceaccount.yaml \
        -f k8s/build/rbac.yaml -f k8s/build/network-policy.yaml
kubectl -n build annotate sa build-spawner eks.amazonaws.com/role-arn="$ROLE_ARN" --overwrite
kubectl create secret generic spawner-secret -n build \
  --from-literal=database-url="postgres://iicpc:devpass@postgres.data.svc.cluster.local:5432/iicpc?sslmode=disable" \
  --from-literal=minio-access-key=minioadmin \
  --from-literal=minio-secret-key=minioadmin123 \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f k8s/build/spawner/deployment.ecr.yaml
kubectl -n build set image deploy/spawner spawner="$REG/iicpc/spawner:$TAG"
kubectl -n build set env   deploy/spawner SPAWNER_IMAGE="$REG/iicpc/spawner:$TAG" REGISTRY_PROVIDER=ecr

# ── 6. frontend (auth REMOVED) + auth-api name-only stub Service (nginx resolves the
#       /api/auth upstream at startup even though auth is off). ──
echo ">> [6/6] frontend (auth removed) + submission-api auth off"
kubectl -n platform set env deploy/submission-api \
  AUTH_REQUIRED=false DEFAULT_CONTESTANT_ID=echo-contestant RESEED_SCENARIOS=false >/dev/null 2>&1 || true
cat <<'YAML' | kubectl apply -f -
apiVersion: v1
kind: Service
metadata: { name: auth-api, namespace: platform }
spec:
  ports: [{ name: http, port: 8080, targetPort: 8080 }]
  selector: { app: auth-api-absent }
YAML
kubectl apply -f k8s/platform/frontend/deployment.yaml -f k8s/platform/frontend/service.yaml
kubectl -n platform set image deploy/frontend frontend="$REG/iicpc/frontend:$TAG"

# ── wait for the data path ──
echo ">> waiting for rollouts"
for d in bot-fleet-worker bot-fleet-controller telemetry-ingester telemetry-rollup correctness-validator score-computer; do
  wait_rollout benchmark "$d" 240s || true
done
wait_rollout sandbox sandbox-orchestrator 240s || true
wait_rollout build spawner 180s || true
wait_rollout platform frontend 180s || true

echo ">> platform up. Next: e2e/03-submission.sh (or submit a zip in the UI) ; e2e/04-scenarios.sh ; e2e/run.sh constant"
