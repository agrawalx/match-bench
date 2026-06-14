#!/usr/bin/env bash
# Full-stack EKS bring-up at one TAG: the load-gen CORE (eks-up.sh) PLUS the three
# layers eks-up omits — eBPF latency capture, the build-worker (zip->image via
# Kaniko->ECR), and the frontend. Auth is disabled end-to-end. Idempotent-ish.
#
# Prereqs: the optimal cluster is already applied (terraform apply with the new
# terraform.tfvars: general/sandbox/botworker pools + the iicpc-prod-build-spawner
# IRSA role), and the v2 image set is in ECR (default tag).
#   TAG=v2 bash deploy-bench/up-full.sh
set -euo pipefail
cd "$(dirname "$0")/.."
export TAG="${TAG:-v2}"
REG="885232248981.dkr.ecr.us-east-1.amazonaws.com"
K="kubectl"
# terraform names this role "<cluster_name>-build-spawner"; account is stable.
ROLE_ARN="arn:aws:iam::885232248981:role/iicpc-prod-build-spawner"

# Benchmark scenario seeded after core is up: one all-HFT firehose (the headline
# throughput number) plus a small realistic-mix layer. 530 tasks < the 1000/worker
# shard limit, so this is exactly ONE worker on ONE c6i.xlarge botworker node.
# Telemetry stays ON so the HDR histograms populate — this is the clean
# telemetry-on measurement run we want.
HFT_TASKS="${HFT_TASKS:-500}"        # 500 x 1000/s  = 500k/s headline
RETAIL="${RETAIL:-10}"               # 10  x 5/s     = 50/s   (market 65 / cancel 5)
INST="${INST:-20}"                   # 20  x 300/s   = 6k/s   (market 20)
SEED_DURATION_S="${SEED_DURATION_S:-120}"

echo "##### 1/5  CORE (data, platform, sandbox, benchmark) #####"
bash deploy-bench/eks-up.sh

echo "##### 2/5  benchmark scenario: ${HFT_TASKS} HFT + ${RETAIL} retail + ${INST} inst, telemetry ON #####"
# Stop submission-api from re-adding the default-mix scenarios on restart, wait
# for that pod to settle, THEN replace the scenarios with the single hft-load one
# (DELETE + INSERT inside seed-hft-load.sh). Ordering avoids a reseed racing the
# delete. Finally clear any stale BOT_DISABLE_TELEMETRY so recording is ON.
${K} -n platform set env deploy/submission-api RESEED_SCENARIOS=false
${K} -n platform rollout status deploy/submission-api --timeout=120s
HFT_TASKS="$HFT_TASKS" RETAIL="$RETAIL" INST="$INST" \
  bash deploy-bench/seed-hft-load.sh 1 "$SEED_DURATION_S"
${K} -n benchmark set env deploy/bot-fleet-worker BOT_DISABLE_TELEMETRY- >/dev/null 2>&1 || true

echo "##### 3/5  eBPF latency capture ON #####"
# The orchestrator spawns the capture sidecar per algo pod when enabled; it already
# has KAFKA_BROKERS from its manifest and resolves the netns on-node.
${K} -n sandbox set env deploy/sandbox-orchestrator \
  CAPTURE_ENABLED=true CAPTURE_IMAGE=$REG/iicpc/ebpf-latency:$TAG
${K} -n sandbox rollout status deploy/sandbox-orchestrator --timeout=120s

echo "##### 4/5  build-worker (zip -> image, Kaniko -> ECR via IRSA) #####"
# base manifests carry the e2e fixes (rbac: secrets verb; netpol: egress to the k8s
# API + port 80 for apt). build topics are created by eks-up's topic-init job.
${K} apply -f k8s/build/namespace.yaml -f k8s/build/serviceaccount.yaml \
        -f k8s/build/rbac.yaml -f k8s/build/network-policy.yaml
${K} -n build annotate sa build-spawner eks.amazonaws.com/role-arn=$ROLE_ARN --overwrite
# spawner-secret: same dev creds eks-up uses for postgres + minio.
${K} create secret generic spawner-secret -n build \
  --from-literal=database-url="postgres://iicpc:devpass@postgres.data.svc.cluster.local:5432/iicpc?sslmode=disable" \
  --from-literal=minio-access-key=minioadmin \
  --from-literal=minio-secret-key=minioadmin123 \
  --dry-run=client -o yaml | ${K} apply -f -
${K} apply -f k8s/build/spawner/deployment.ecr.yaml
${K} -n build set image deploy/spawner spawner=$REG/iicpc/spawner:$TAG
${K} -n build set env   deploy/spawner SPAWNER_IMAGE=$REG/iicpc/spawner:$TAG REGISTRY_PROVIDER=ecr
${K} -n build rollout status deploy/spawner --timeout=180s

echo "##### 5/5  frontend (auth disabled) + auth-api stub #####"
# The frontend nginx resolves leaderboard-api/submission-api/auth-api upstreams at
# startup; auth is off so auth-api is a name-only stub Service (no backend) just so
# nginx starts. Keep the default contestant aligned with the frontend's baked value.
${K} -n platform set env deploy/submission-api DEFAULT_CONTESTANT_ID=echo-contestant >/dev/null 2>&1 || true
cat <<'YAML' | ${K} apply -f -
apiVersion: v1
kind: Service
metadata:
  name: auth-api
  namespace: platform
spec:
  ports:
    - name: http
      port: 8080
      targetPort: 8080
  selector:
    app: auth-api-absent
YAML
${K} apply -f k8s/platform/frontend/deployment.yaml -f k8s/platform/frontend/service.yaml
${K} -n platform set image deploy/frontend frontend=$REG/iicpc/frontend:$TAG
${K} -n platform rollout status deploy/frontend --timeout=180s

echo "##### FULL STACK UP (TAG=$TAG) #####"
echo "  scenario : ${HFT_TASKS} HFT + ${RETAIL} retail + ${INST} inst (~$((HFT_TASKS*1000 + RETAIL*5 + INST*300))/s), ${SEED_DURATION_S}s, telemetry ON"
echo "  frontend : kubectl -n platform port-forward svc/frontend 8087:8080   -> http://127.0.0.1:8087"
echo "  grafana  : kubectl -n observability port-forward svc/grafana 3000:3000  (admin/admin)"
echo "  submit   : upload e2e/contestant-fix-acker.zip in the UI (auth off) -> build -> run"
echo "  measure  : watch bot-fleet 'orders sent /s' (does it hold ~$((HFT_TASKS*1000))/s?) vs"
echo "             telemetry-ingester consumed/s — that gap is the telemetry-on ceiling."
