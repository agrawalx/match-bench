#!/usr/bin/env bash
# Shared helpers for the local-cluster verification scripts. Source this:
#   . deploy-local/lib-local.sh
#
# This is e2e/lib.sh minus AWS: that one calls `aws sts get-caller-identity` at
# source time, so it aborts under `set -e` on a machine with no AWS credentials —
# which is every machine running the local track.
set -euo pipefail
export REPO_ROOT="$(git rev-parse --show-toplevel)"
K="${K:-kubectl}"

# psql_exec <sql> — run SQL against the platform Postgres (iicpc db).
psql_exec() {
  ${K} exec -i -n data postgres-0 -- psql -U iicpc -d iicpc -v ON_ERROR_STOP=1 -c "$1" \
    2>&1 | grep -vE "Defaulted" || true
}

# psql_val <sql> — single scalar, no headers/padding. Empty string when no row.
psql_val() {
  ${K} exec -i -n data postgres-0 -- psql -U iicpc -d iicpc -tAq -c "$1" 2>/dev/null \
    | grep -vE "^Defaulted" | tr -d '\r' | head -1
}

# ts_val <sql> — single scalar from TimescaleDB (metrics db).
ts_val() {
  ${K} exec -i -n data timescaledb-0 -- psql -U iicpc -d metrics -tAq -c "$1" 2>/dev/null \
    | grep -vE "^Defaulted" | tr -d '\r' | head -1
}

# kafka_produce <topic> <key> <value>
kafka_produce() {
  ${K} exec -n data kafka-0 -- bash -c \
    "echo '$2:$3' | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 \
       --topic '$1' --property parse.key=true --property key.separator=:" \
    2>&1 | grep -vE "Defaulted|^$" || true
}

# new_session_id — uuidv7-ish (time-ordered), matching what the platform expects
# on benchmark.requested.
new_session_id() {
  local ms ts rnd
  ms=$(( $(date +%s%N)/1000000 )); ts=$(printf '%012x' "$ms")
  rnd=$(cat /proc/sys/kernel/random/uuid | tr -d '-')
  echo "${ts:0:8}-${ts:8:4}-7${rnd:1:3}-8${rnd:5:3}-${rnd:8:12}"
}

# prom_metrics <ns> <app-label> [port] — dump a pod's whole /metrics to stdout.
# Goes through a short-lived port-forward rather than `kubectl exec ... wget`: the
# service images are distroless-ish (debian-slim, no wget/curl), so exec-ing a
# fetcher into them does not work.
prom_metrics() {
  local ns="$1" app="$2" port="${3:-9090}" pod lport pf_pid
  pod="$(${K} -n "$ns" get pod -l "app=$app" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)"
  [ -z "$pod" ] && return 0
  lport=$(( 20000 + RANDOM % 10000 ))
  ${K} -n "$ns" port-forward "pod/$pod" "${lport}:${port}" >/dev/null 2>&1 &
  pf_pid=$!
  for _ in $(seq 1 20); do
    curl -sf "http://127.0.0.1:${lport}/metrics" 2>/dev/null && break
    sleep 0.25
  done
  kill "$pf_pid" 2>/dev/null || true
  wait "$pf_pid" 2>/dev/null || true
}

# prom_value <metrics-text> <metric-name> — sum every sample of a metric (across
# label sets). Prints 0 when the metric is absent, so callers can compare
# numerically without special-casing "never incremented".
#
# The `|| true` is load-bearing, not defensive noise: these scripts run under
# `set -euo pipefail`, and a grep that matches nothing exits 1, which pipefail
# propagates out of the command substitution and `set -e` turns into an immediate
# script exit. A metric that has not been emitted yet is the NORMAL state early in a
# run, so without this the caller dies on its first poll.
#
# Metric names carry the `iicpc_` prefix the metrics library adds, so callers should
# pass e.g. iicpc_controller_leased_partitions — matching on the bare name silently
# finds nothing.
prom_value() {
  { echo "$1" | grep -E "^$2([ {])" || true; } | awk '{s+=$NF} END{printf "%.0f", s+0}'
}

# ok / fail — assertion output plus a counter the caller checks at the end.
ASSERT_FAILURES=0
ok()   { echo "  PASS  $*"; }
fail() { echo "  FAIL  $*"; ASSERT_FAILURES=$((ASSERT_FAILURES+1)); }
