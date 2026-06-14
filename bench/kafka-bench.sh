#!/usr/bin/env bash
# Bring up the multi-broker Kafka tier for the BENCH suite on the dedicated `kafka`
# node pool, replacing any single-broker Kafka, plus 96-partition topics and the
# bot/ingester scaled for ~2M/s. Idempotent: deletes + recreates the Kafka
# StatefulSet/PVCs (broker logs are transient benchmark data).
#
# Defaults: KBROKERS=2, DATA_RF=1 (no replication — correct for transient bench data;
# 700 MB/s split over 2 brokers @ gp3-500 is ample). Bump KBROKERS=3 for headroom.
set -euo pipefail
. "$(dirname "$0")/../e2e/lib.sh"
cd "$REPO_ROOT"
KBROKERS="${KBROKERS:-2}"
DATA_RF="${DATA_RF:-1}"
INTERNAL_RF=$(( KBROKERS < 2 ? 1 : 2 ))   # offsets/txn topics survive 1 broker loss when possible

echo ">> bench Kafka: $KBROKERS brokers, data RF=$DATA_RF, internal RF=$INTERNAL_RF"

echo ">> 1. high-throughput gp3 StorageClass for broker logs (500 MB/s)"
cat <<'YAML' | kubectl apply -f -
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: gp3-bench
provisioner: ebs.csi.aws.com
volumeBindingMode: WaitForFirstConsumer
allowVolumeExpansion: true
parameters:
  type: gp3
  throughput: "500"
  iops: "5000"
  fsType: ext4
YAML

echo ">> 2. shared KRaft cluster-id secret (all brokers use it)"
kubectl get secret kafka-secret -n data >/dev/null 2>&1 || kubectl -n data create secret generic kafka-secret \
  --from-literal=cluster-id="$(cat /proc/sys/kernel/random/uuid | tr -d '-' | head -c 22)"

echo ">> 3. tear down any existing Kafka + its PVCs (transient logs)"
kubectl -n data delete statefulset kafka --ignore-not-found --wait=true
kubectl -n data delete pvc -l app=kafka --ignore-not-found

echo ">> 4. apply the $KBROKERS-broker StatefulSet on the kafka pool"
kubectl apply -f k8s/data/kafka/service.yaml
KBROKERS="$KBROKERS" INTERNAL_RF="$INTERNAL_RF" python3 - <<'PY' | kubectl apply -f -
import os, yaml
n = int(os.environ["KBROKERS"]); irf = os.environ["INTERNAL_RF"]
d = yaml.safe_load(open("k8s/data/kafka/statefulset.yaml"))
d["spec"]["replicas"] = n
sp = d["spec"]["template"]["spec"]
sp["nodeSelector"] = {"pool": "kafka"}
sp["tolerations"] = [{"key": "kafka", "operator": "Equal", "value": "true", "effect": "NoSchedule"}]
sp["affinity"] = {"podAntiAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": [
    {"labelSelector": {"matchLabels": {"app": "kafka"}}, "topologyKey": "kubernetes.io/hostname"}]}}
voters = ",".join(f"{i}@kafka-{i}.kafka.data.svc.cluster.local:9093" for i in range(n))
env_over = {
    "KAFKA_CONTROLLER_QUORUM_VOTERS": voters,
    "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR": irf,
    "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": irf,
    "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR": "1",
    "KAFKA_MIN_INSYNC_REPLICAS": "1",
}
for c in sp["containers"]:
    if c["name"] == "kafka":
        for e in c["env"]:
            if e.get("name") in env_over:
                e["value"] = env_over.pop(e["name"])
        c["resources"] = {"requests": {"cpu": "2", "memory": "6Gi"},
                          "limits": {"cpu": "3500m", "memory": "12Gi"}}
vct = d["spec"]["volumeClaimTemplates"][0]
vct["spec"]["storageClassName"] = "gp3-bench"
vct["spec"]["resources"]["requests"]["storage"] = "200Gi"
print(yaml.safe_dump(d))
PY
kubectl -n data rollout status statefulset/kafka --timeout=500s

echo ">> 5. topics: 96 partitions, RF=$DATA_RF"
kubectl -n data delete job kafka-topic-init --ignore-not-found
sed -e 's/orders.sent 24/orders.sent 96/' -e 's/orders.acked 24/orders.acked 96/' \
    -e "s/--replication-factor 3/--replication-factor $DATA_RF/" \
    -e 's/min.insync.replicas=2/min.insync.replicas=1/' \
    k8s/data/kafka/topic-init-job.yaml | kubectl apply -f -
kubectl -n data wait --for=condition=complete job/kafka-topic-init --timeout=180s \
  || kubectl -n data logs job/kafka-topic-init | tail

echo ">> 6. scale producers/consumers for 96 partitions / 2M/s"
kubectl -n benchmark set env deploy/bot-fleet-worker ORDERS_PARTITIONS=96 2>/dev/null || true
kubectl -n sandbox set env deploy/sandbox-orchestrator ORDERS_PARTITIONS=96 2>/dev/null || true
kubectl -n benchmark scale deploy/telemetry-ingester --replicas=8 2>/dev/null || true

echo ">> $KBROKERS-broker Kafka up; 96-partition RF=$DATA_RF topics; ingester x8; ORDERS_PARTITIONS=96"
echo ">> verify quorum: kubectl -n data exec kafka-0 -- /opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server localhost:9092 describe --status"
