#!/usr/bin/env bash
# infra/scripts/netpol-deny-test.sh
#
# This script automates infrastructure operations for netpol deny test.
# It belongs to the IICPC operational toolchain and should keep
# setup, validation, and deployment behavior explicit at entry points.
# Function-level comments describe reusable shell routines below.

set -euo pipefail

NS=sandbox
POD=netpol-deny-test
IMAGE=nicolaka/netshoot:latest
PG_HOST=postgres.data.svc.cluster.local
PG_PORT=5432

# cleanup performs the script-specific operation described by its name.
# It keeps command side effects explicit and returns shell status to callers.
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
