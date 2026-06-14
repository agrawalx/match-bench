#!/usr/bin/env bash
# Benchmark-only patches, applied AFTER the platform is up on EKS, to turn the
# prod deployment into a controlled load-generator scaling sweep. These are NOT
# baked into the shared manifests on purpose: pinning bot-fleet to the botworker
# pool would break local k3s (no such node label) and the prod KEDA flow.
#
# What it does:
#   1. Pin bot-fleet-worker to the dedicated `botworker` node group (nodeSelector
#      + toleration for the botworker taint) so load gens never contend with the
#      data/measurement plane or the contestant.
#   2. One worker pod PER botworker node (podAntiAffinity on hostname) so a worker
#      gets the whole c6i.xlarge (its cpu limit is already "4" = the full node).
#   3. Disable KEDA and pin a FIXED replica count == botworker node count, so the
#      ONLY variable in the sweep is node count (KEDA lag-autoscaling would
#      otherwise move replicas mid-run and confound the measurement).
#
# Usage:
#   deploy-bench/bench-patches.sh 1     # sweep point A: 1 load-gen node + 1 worker
#   deploy-bench/bench-patches.sh 2     # sweep point B: 2 load-gen nodes + 2 workers
# Pair each with the matching Terraform:  terraform apply -var botworker_desired_size=N
set -euo pipefail
N="${1:?usage: bench-patches.sh <botworker_node_count>}"
K="kubectl"
NS=benchmark

echo "== 1. disable KEDA autoscaling (fixed replicas for a deterministic sweep) =="
${K} -n "$NS" delete scaledobject bot-fleet-worker --ignore-not-found

echo "== 2. pin bot-fleet-worker to the botworker pool, one pod per node =="
${K} -n "$NS" patch deployment bot-fleet-worker --type merge -p "$(cat <<EOF
spec:
  replicas: ${N}
  template:
    spec:
      nodeSelector:
        pool: botworker
      tolerations:
        - key: botworker
          operator: Equal
          value: "true"
          effect: NoSchedule
      affinity:
        podAntiAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
            - labelSelector:
                matchLabels:
                  app: bot-fleet-worker
              topologyKey: kubernetes.io/hostname
EOF
)"

echo "== 3. wait for rollout =="
${K} -n "$NS" rollout status deployment/bot-fleet-worker --timeout=180s

echo
echo ">> ${N} worker(s) pinned, one per botworker node. Verify placement:"
echo "   kubectl -n ${NS} get pods -l app=bot-fleet-worker -o wide"
echo ">> Drive a run, then read the numbers (per-task rate, aggregate orders/s,"
echo "   p99) from Grafana + the metrics table. Watch Kafka broker disk I/O and"
echo "   consumer lag — if Kafka pins, it's the bottleneck, not the load gens."
