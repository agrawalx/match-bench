#!/usr/bin/env bash
# netpol-deny-test.sh — the HARD GATE from DEPLOYMENT_EKS.md §3.1 / §13.1.
#
# Problem: on stock EKS the VPC CNI ignores NetworkPolicy objects entirely, so
# `kubectl get netpol` shows every isolation policy present while NOTHING is
# enforced. "The policy exists" and "the policy is enforced" are different facts;
# only the second protects contestants. The sandbox-isolation policy
# (k8s/sandbox/network-policy.yaml) blocks ALL cluster egress from algo pods —
# they must not reach postgres/kafka/minio or other contestants.
#
# Decision: prove enforcement empirically. Spawn a throwaway pod in the `sandbox`
# namespace (subject to sandbox-isolation, no ebpf-capture label) and assert:
#   1. it CANNOT open postgres.data:5432   (egress to cluster -> must FAIL)
#   2. it CAN resolve DNS + reach the public internet (egress allowlist -> must PASS)
# If (1) succeeds in connecting, enforcement is OFF — exit non-zero and STOP the
# deploy.
#
# Why a real pod (not a dry-run): the whole point is that the data plane, not the
# API server, is what enforces. Requires the data tier (postgres) to be up, or
# step 1 is inconclusive; run this AFTER the data tier and CNI are deployed.
set -euo pipefail

NS=sandbox
POD=netpol-deny-test
IMAGE=nicolaka/netshoot:latest # has nc, dig, curl
PG_HOST=postgres.data.svc.cluster.local
PG_PORT=5432

cleanup() { kubectl -n "$NS" delete pod "$POD" --ignore-not-found --wait=false >/dev/null 2>&1 || true; }
trap cleanup EXIT

echo ">> launching probe pod in ns/$NS (subject to sandbox-isolation)"
kubectl -n "$NS" delete pod "$POD" --ignore-not-found --wait=true >/dev/null 2>&1 || true
kubectl -n "$NS" run "$POD" --image="$IMAGE" --restart=Never \
  --overrides='{"spec":{"tolerations":[{"key":"sandbox","operator":"Equal","value":"true","effect":"NoSchedule"}],"nodeSelector":{"pool":"sandbox"}}}' \
  --command -- sleep 600 >/dev/null
kubectl -n "$NS" wait --for=condition=Ready "pod/$POD" --timeout=120s

fail=0

echo ">> [1/2] cluster egress MUST be denied: $PG_HOST:$PG_PORT"
if kubectl -n "$NS" exec "$POD" -- nc -z -w 5 "$PG_HOST" "$PG_PORT" >/dev/null 2>&1; then
  echo "   FAIL: connected to postgres from a sandbox pod — NetworkPolicy is NOT enforced."
  echo "         Enable VPC CNI NetworkPolicy (enableNetworkPolicy=true) or install Calico."
  fail=1
else
  echo "   PASS: postgres unreachable from sandbox pod (egress denied)."
fi

echo ">> [2/2] public internet egress MUST be allowed (allowlist + DNS)"
if kubectl -n "$NS" exec "$POD" -- sh -c 'curl -fsS --max-time 8 https://checkip.amazonaws.com >/dev/null'; then
  echo "   PASS: public egress works (allowlist + DNS intact)."
else
  echo "   WARN: public egress failed — DNS or the 0.0.0.0/0 allowlist may be broken."
  echo "         (This does not by itself mean isolation is off, but the algo pods need it.)"
  fail=1
fi

if [ "$fail" -ne 0 ]; then
  echo ">> GATE FAILED. Do not proceed with the deploy."
  exit 1
fi
echo ">> GATE PASSED. Sandbox isolation is enforced."
