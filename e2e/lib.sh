#!/usr/bin/env bash
# Shared helpers for the e2e suite. Source this: `. e2e/lib.sh`
set -euo pipefail
export AWS_PROFILE="${AWS_PROFILE:-iicpc}"
export AWS_REGION="${AWS_REGION:-us-east-1}"
ACCOUNT="$(aws sts get-caller-identity --query Account --output text 2>/dev/null)"
export REG="${ACCOUNT}.dkr.ecr.${AWS_REGION}.amazonaws.com"
export REPO_ROOT="$(git rev-parse --show-toplevel)"

ecr_login() {
  aws ecr get-login-password --region "$AWS_REGION" \
    | docker login --username AWS --password-stdin "$REG" >/dev/null
}

# wait_rollout <ns> <deploy> [timeout]
wait_rollout() {
  kubectl -n "$1" rollout status "deploy/$2" --timeout="${3:-180s}"
}

# psql_exec <sql>
psql_exec() {
  kubectl exec -i -n data postgres-0 -- psql -U iicpc -d iicpc -v ON_ERROR_STOP=1 -c "$1" \
    2>&1 | grep -vE "Defaulted" || true
}

# kafka_produce <topic> <key> <value>
kafka_produce() {
  kubectl exec -n data kafka-0 -- bash -c \
    "echo '$2:$3' | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 \
       --topic '$1' --property parse.key=true --property key.separator=:" \
    2>&1 | grep -vE "Defaulted|^$" || true
}

# uuidv7-ish session id (time-ordered) for benchmark.requested
new_session_id() {
  local ms ts rnd
  ms=$(( $(date +%s%N)/1000000 )); ts=$(printf '%012x' "$ms")
  rnd=$(cat /proc/sys/kernel/random/uuid | tr -d '-')
  echo "${ts:0:8}-${ts:8:4}-7${rnd:1:3}-8${rnd:5:3}-${rnd:8:12}"
}
