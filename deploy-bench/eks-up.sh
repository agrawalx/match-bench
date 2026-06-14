#!/usr/bin/env bash
# EKS platform deploy for the load-gen (sent/s) benchmark. Scripted equivalent of
# the infra/Makefile deploy target (make is unusable on this host). Mirrors
# deploy-local/up.sh but for EKS: ECR images (tag = git SHA), gp3 default storage,
# SINGLE-broker Kafka, gVisor + eBPF capture OFF, no in-cluster registry, no build
# tier, no public ALB ingress. Idempotent-ish (apply + set image/env).
set -euo pipefail
cd "$(dirname "$0")/.."
K="kubectl"
REG="885232248981.dkr.ecr.us-east-1.amazonaws.com"
TAG="${TAG:-v2}"   # unified image tag for the optimal-config cluster (override: TAG=... )
echo ">> registry=$REG tag=$TAG"

# dev creds (in-cluster DNS, same as local) ---------------------------------
PG_PW=devpass; TS_PW=devpass; MINIO_AK=minioadmin; MINIO_SK=minioadmin123
DB="postgres://iicpc:${PG_PW}@postgres.data.svc.cluster.local:5432/iicpc?sslmode=disable"
TSDB="postgres://iicpc:${TS_PW}@timescaledb.data.svc.cluster.local:5432/metrics?sslmode=disable"
KB="kafka.data.svc.cluster.local:9092"
REDIS_ADDR="redis.data.svc.cluster.local:6379"; REDIS_URL="redis://redis.data.svc.cluster.local:6379"
mk(){ ${K} create secret generic "$1" -n "$2" "${@:3}" --dry-run=client -o yaml | ${K} apply -f -; }
# apply a dir's manifests, skipping netpol / namespace / public ingress
applynp(){ find "$1" -name '*.yaml' ! -name '*network-policy*' ! -name 'namespace.yaml' ! -name '*ingress*' -print0 | xargs -0 -I{} ${K} apply -f {}; }

echo "== 1. namespaces =="
for ns in data platform sandbox benchmark observability; do ${K} apply -f k8s/$ns/namespace.yaml; done

echo "== 2. secrets =="
mk postgres-secret data --from-literal=password=$PG_PW
mk timescaledb-secret data --from-literal=password=$TS_PW
mk minio-secret data --from-literal=access-key=$MINIO_AK --from-literal=secret-key=$MINIO_SK
KID=$(python3 -c "import uuid,base64;print(base64.urlsafe_b64encode(uuid.uuid4().bytes+uuid.uuid4().bytes[:4]).decode().rstrip('='))")
mk kafka-secret data --from-literal=cluster-id=$KID
mk submission-api-secret platform --from-literal=database-url="$DB" --from-literal=minio-access-key=$MINIO_AK --from-literal=minio-secret-key=$MINIO_SK
mk leaderboard-api-secret platform --from-literal=database-url="$DB" --from-literal=timescale-url="$TSDB" --from-literal=kafka-brokers="$KB" --from-literal=redis-addr="$REDIS_ADDR"
mk bot-fleet-controller-secret benchmark --from-literal=database-url="$DB"
mk correctness-validator-secret benchmark --from-literal=kafka-brokers="$KB" --from-literal=database-url="$DB"
mk score-computer-secret benchmark --from-literal=database-url="$DB" --from-literal=timescale-url="$TSDB" --from-literal=kafka-brokers="$KB" --from-literal=redis-addr="$REDIS_ADDR"
mk telemetry-ingester-secrets benchmark --from-literal=KAFKA_BROKERS="$KB" --from-literal=TIMESCALE_URL="$TSDB" --from-literal=REDIS_URL="$REDIS_URL"
mk grafana-admin observability --from-literal=admin-user=admin --from-literal=admin-password=admin
# submission-api hard-references GOOGLE_CLIENT_ID from auth-api-secret (Optional:false)
# even with AUTH_REQUIRED=false, so it must exist or the container won't start. Dummy
# value is fine since auth is off and we don't deploy auth-api/frontend.
mk auth-api-secret platform --from-literal=google-client-id=dummy

echo "== 3. data tier (Kafka: 2 brokers / RF=1, anti-affinity one-per-node) =="
${K} apply -f k8s/data/postgres/service.yaml -f k8s/data/postgres/statefulset.yaml
${K} apply -f k8s/data/timescaledb/service.yaml -f k8s/data/timescaledb/statefulset.yaml
${K} apply -f k8s/data/minio/service.yaml -f k8s/data/minio/statefulset.yaml
${K} apply -f k8s/data/redis/service.yaml -f k8s/data/redis/statefulset.yaml
${K} apply -f k8s/data/kafka/service.yaml
# 2 brokers (RF=1): the orders.sent/orders.acked 24 partitions auto-spread 12/12,
# ~doubling produce-accept vs a single broker (the telemetry-on send ceiling was
# ~445k/s on one broker). podAntiAffinity (in the manifest) pins one broker per
# node, so this needs 2 schedulable nodes (general has 3). RF stays 1 (a benchmark
# doesn't need replication); a broker loss drops its partitions, acceptable here.
sed -e 's/replicas: 3/replicas: 2/' \
    -e 's#0@kafka-0.kafka.data.svc.cluster.local:9093,1@kafka-1.kafka.data.svc.cluster.local:9093,2@kafka-2.kafka.data.svc.cluster.local:9093#0@kafka-0.kafka.data.svc.cluster.local:9093,1@kafka-1.kafka.data.svc.cluster.local:9093#' \
    -e 's/value: "3"/value: "1"/g' -e 's/value: "2"/value: "1"/g' \
    k8s/data/kafka/statefulset.yaml | ${K} apply -f -

echo ">> waiting for data tier (EBS provisioning ~1-2 min each)..."
${K} -n data rollout status statefulset/postgres --timeout=300s
${K} -n data rollout status statefulset/timescaledb --timeout=300s
${K} -n data rollout status statefulset/kafka --timeout=300s
${K} -n data rollout status statefulset/minio --timeout=180s || true
${K} -n data rollout status statefulset/redis --timeout=180s || true

echo "== 4. kafka topics (RF=1) =="
sed -e 's/--replication-factor 3/--replication-factor 1/' -e 's/min.insync.replicas=2/min.insync.replicas=1/' \
    k8s/data/kafka/topic-init-job.yaml | ${K} apply -f -
${K} -n data wait --for=condition=complete job/kafka-topic-init --timeout=180s || ${K} -n data logs job/kafka-topic-init | tail

echo "== 5. observability (Prometheus/Grafana/Loki) =="
applynp k8s/observability
${K} apply -f deploy-local/grafana-timescale-datasource.yaml 2>/dev/null || true
${K} -n observability rollout restart deployment/grafana 2>/dev/null || true

echo "== 6. platform: submission-api (schema + trigger) + leaderboard-api, AUTH off =="
applynp k8s/platform/submission-api
${K} -n platform set image deployment/submission-api submission-api=$REG/iicpc/submission-api:$TAG
${K} -n platform set env deployment/submission-api AUTH_REQUIRED=false RESEED_SCENARIOS=true
applynp k8s/platform/leaderboard-api
${K} -n platform set image deployment/leaderboard-api leaderboard-api=$REG/iicpc/leaderboard-api:$TAG

echo "== 7. sandbox orchestrator (runc, capture off, ECR) =="
${K} apply -f k8s/sandbox/sandbox-orchestrator/rbac.yaml
applynp k8s/sandbox/sandbox-orchestrator
${K} -n sandbox set image deployment/sandbox-orchestrator sandbox-orchestrator=$REG/iicpc/sandbox-orchestrator:$TAG
# SANDBOX_NODE_POOL=sandbox: the sandbox node group is tainted sandbox=true:NoSchedule
# and labelled pool=sandbox. The orchestrator only adds the matching toleration +
# nodeSelector to algo pods when this is set (local k3s unsets it — single untainted
# node). Without it, algo pods can't schedule on the sandbox node and fall back to the
# general nodes, which can't fit ALGO_CPU cores → FailedScheduling.
${K} -n sandbox set env deployment/sandbox-orchestrator RUNTIME_CLASS- CAPTURE_ENABLED=false SANDBOX_NODE_POOL=sandbox

echo "== 8. benchmark tier (controller, worker, ingester+rollup; ECR) =="
applynp k8s/benchmark/telemetry-ingester
${K} -n benchmark set image deployment/telemetry-ingester telemetry-ingester=$REG/iicpc/telemetry-ingester:$TAG || true
${K} -n benchmark set image deployment/telemetry-rollup    telemetry-rollup=$REG/iicpc/telemetry-ingester:$TAG || true
applynp k8s/benchmark/correctness-validator
${K} -n benchmark set image deployment/correctness-validator correctness-validator=$REG/iicpc/correctness-validator:$TAG || true
applynp k8s/benchmark/score-computer
${K} -n benchmark set image deployment/score-computer score-computer=$REG/iicpc/score-computer:$TAG || true
${K} apply -f k8s/benchmark/bot-fleet-controller/deployment.yaml
${K} -n benchmark set image deployment/bot-fleet-controller bot-fleet-controller=$REG/iicpc/bot-fleet-controller:$TAG
# READY_DEADLINE=120s: scenarios in a run-group serialize on the single worker, so the
# next scenario's ready-fan-in must wait out the previous run (the 30s default failed ramp).
${K} -n benchmark set env deployment/bot-fleet-controller SANDBOX_ORCHESTRATOR_URL=http://sandbox-orchestrator.sandbox.svc.cluster.local:8080 MAX_TASKS_PER_WORKER=1000 READY_DEADLINE=120s
applynp k8s/benchmark/bot-fleet
${K} -n benchmark set image deployment/bot-fleet-worker bot-fleet-worker=$REG/iicpc/bot-fleet:$TAG

echo ">> waiting for core services..."
for d in platform/submission-api benchmark/bot-fleet-controller sandbox/sandbox-orchestrator benchmark/telemetry-ingester observability/grafana observability/prometheus; do
  ns=${d%/*}; n=${d#*/}; ${K} -n $ns rollout status deployment/$n --timeout=240s || echo "WARN: $d not ready"
done
echo "== EKS PLATFORM UP =="
