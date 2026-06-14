#!/usr/bin/env bash
# Register the drain sink as a "ready" submission so the controller will deploy it
# on the sandbox node and point the bots at it — no upload/build pipeline needed.
# Also flips off the bits that are irrelevant to a sent/s run:
#   - eBPF capture (the drain never replies, so there's nothing to capture; off
#     frees the sandbox node's cores for the drain's read loop).
#   - submission-api scenario reseeding (so it can't re-add the default scenarios
#     on top of the hft-load one).
#
#   DRAIN_IMAGE=<ref> deploy-bench/seed-drain-submission.sh
# Prints the submission_id to feed run-sentps.sh.
set -euo pipefail
DRAIN_IMAGE="${DRAIN_IMAGE:-192.168.1.11:5000/iicpc/contestant-drain:bench}"
PGNS=data; PGPOD=postgres-0; PGUSER=iicpc; PGDB=iicpc

SID="drain-$(cat /proc/sys/kernel/random/uuid)"
SHA="$(cat /proc/sys/kernel/random/uuid | tr -d '-')$(cat /proc/sys/kernel/random/uuid | tr -d '-')"  # dummy unique sha256

echo ">> registering drain submission $SID -> $DRAIN_IMAGE (FIX, :9898, ready)"
kubectl exec -i -n "$PGNS" "$PGPOD" -- psql -U "$PGUSER" -d "$PGDB" -v ON_ERROR_STOP=1 <<SQL
INSERT INTO submissions
  (submission_id, contestant_id, sha256, language, protocol, port, team_name, artifact_path, image_ref, status)
VALUES
  ('$SID', 'benchmark', '$SHA', 'rust', 'FIX', 9898, 'loadgen-bench', 'n/a', '$DRAIN_IMAGE', 'ready');
SQL

echo ">> disabling eBPF capture; giving the drain headroom; turning off reseeding"
# Capture off: the drain never replies, nothing to capture; frees sandbox cores.
kubectl -n sandbox set env deploy/sandbox-orchestrator CAPTURE_ENABLED=false >/dev/null
# ALGO_CPU=2: the optimal-config sandbox is c6i.xlarge (4 vCPU) — contestant gets 2,
# the other 2 run capture/system. (Was 6, sized for the old 8-vCPU c6i.2xlarge sandbox;
# 6 won't schedule on 4 vCPU → FailedScheduling.) A read-and-discard drain still
# out-absorbs the load gens at 2 vCPU since it does no work per byte. Integer = cpuset/
# Guaranteed QoS friendly.
kubectl -n sandbox set env deploy/sandbox-orchestrator ALGO_CPU=2 >/dev/null
# Reseed off: don't let a submission-api restart re-add the default scenarios.
kubectl -n platform set env deploy/submission-api RESEED_SCENARIOS=false >/dev/null 2>&1 || true

echo
echo "SUBMISSION_ID=$SID"
echo ">> next: deploy-bench/seed-hft-load.sh <N>  then  deploy-bench/run-sentps.sh $SID"
