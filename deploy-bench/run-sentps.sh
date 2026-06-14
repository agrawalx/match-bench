#!/usr/bin/env bash
# Trigger one load-generator capacity run against the drain sink and sample the
# ONLY metric we care about: orders sent/s (iicpc_bot_orders_sent, bot-side).
#
# Publishes benchmark.requested directly to Kafka (bypasses submission-api/auth);
# the controller deploys the drain on the sandbox node and dispatches the hft-load
# scenario to the pinned worker(s). Then it polls Prometheus for the aggregate
# send rate until the session ends.
#
#   deploy-bench/run-sentps.sh <submission_id> [scenario_id]
# Requires Prometheus reachable at $PROM (default localhost:9090 — deploy-local/
# forward.sh, or kubectl -n observability port-forward svc/prometheus 9090:9090).
set -euo pipefail
SUB="${1:?usage: run-sentps.sh <submission_id> [scenario_id]}"
SCENARIO="${2:-hft-load}"
PROM="${PROM:-http://localhost:9090}"
# SAMPLE_SECONDS=0 (default) → sample forever (interactive). >0 → bounded mode for
# the scaling sweep: sample for that many seconds, then print a machine-readable
# "RESULT peak_sent_s=<n> last_sent_s=<n>" line and exit.
SAMPLE_SECONDS="${SAMPLE_SECONDS:-0}"
KNS=data; KPOD=kafka-0; BROKER=localhost:9092

# session_id is UUIDv7 (the validator's drain bounds on its embedded timestamp —
# irrelevant here, but keep it well-formed). run_group_id is a plain uuid.
ms=$(( $(date +%s%N) / 1000000 )); ts=$(printf '%012x' "$ms"); rnd=$(cat /proc/sys/kernel/random/uuid | tr -d '-')
SESSION="${ts:0:8}-${ts:8:4}-7${rnd:1:3}-8${rnd:5:3}-${rnd:8:12}"
RGID=$(cat /proc/sys/kernel/random/uuid)
NOW=$(date -u +%Y-%m-%dT%H:%M:%SZ)
MSG="{\"session_id\":\"$SESSION\",\"submission_id\":\"$SUB\",\"contestant_id\":\"benchmark\",\"run_group_id\":\"$RGID\",\"scenario_id\":\"$SCENARIO\",\"requested_at\":\"$NOW\"}"

echo ">> publishing benchmark.requested (session=$SESSION scenario=$SCENARIO)"
kubectl exec -n "$KNS" "$KPOD" -- bash -c \
  "echo '$RGID:$MSG' | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server $BROKER --topic benchmark.requested --property parse.key=true --property key.separator=:" \
  2>&1 | grep -vE "Defaulted|^$" || true

if [ "$SAMPLE_SECONDS" -gt 0 ] 2>/dev/null; then
  echo ">> sampling orders sent/s for ${SAMPLE_SECONDS}s (bounded sweep mode)."
else
  echo ">> sampling orders sent/s (Ctrl-C to stop). Columns: total/s and per-task avg."
fi
echo "   (drain deploy + barrier warmup take ~30-60s before the rate ramps)"
# pq <promql> -> integer instant value (0 on miss/parse error). Keeps the loop DRY.
pq() {
  curl -sG "$PROM/api/v1/query" --data-urlencode "query=$1" \
    | python3 -c "import sys,json;r=json.load(sys.stdin)['data']['result'];print(int(float(r[0]['value'][1])) if r else 0)" 2>/dev/null || echo 0
}
q_total='sum(rate(iicpc_bot_orders_sent_total[15s]))'
q_tasks='sum(iicpc_bot_tasks_assigned_total)'
# Telemetry-pipeline rates (only sampled when TELEMETRY_METRICS=1): worker->Kafka
# delivered, worker drops (should be ~0 — record() is lossless/blocking), and the
# ingester's consume rate. With telemetry ON these are the real ceiling, not sent/s.
q_flush='sum(rate(iicpc_bot_telemetry_events_flushed_total[15s]))'
q_drop='sum(rate(iicpc_bot_telemetry_events_dropped_total[15s]))'
q_cons='sum(rate(iicpc_telemetry_events_consumed_total[15s]))'
TELEMETRY_METRICS="${TELEMETRY_METRICS:-0}"
start=$(date +%s); peak=0; last=0; peak_flush=0; peak_cons=0; peak_drop=0
while true; do
  total=$(pq "$q_total"); tasks=$(pq "$q_tasks")
  last=$total
  [ "$total" -gt "$peak" ] 2>/dev/null && peak=$total
  per=0; [ "${tasks:-0}" -gt 0 ] 2>/dev/null && per=$(( total / tasks ))
  if [ "$TELEMETRY_METRICS" = 1 ]; then
    flush=$(pq "$q_flush"); cons=$(pq "$q_cons"); drop=$(pq "$q_drop")
    [ "$flush" -gt "$peak_flush" ] 2>/dev/null && peak_flush=$flush
    [ "$cons"  -gt "$peak_cons"  ] 2>/dev/null && peak_cons=$cons
    [ "$drop"  -gt "$peak_drop"  ] 2>/dev/null && peak_drop=$drop
    printf "   %s  sent/s=%-9s flushed/s=%-9s consumed/s=%-9s dropped/s=%-7s peakSent=%s\n" \
      "$(date +%H:%M:%S)" "$total" "$flush" "$cons" "$drop" "$peak"
  else
    printf "   %s  sent/s=%-9s  tasks=%-6s  per-task=%-5s  peak=%s\n" "$(date +%H:%M:%S)" "$total" "$tasks" "$per" "$peak"
  fi
  if [ "$SAMPLE_SECONDS" -gt 0 ] 2>/dev/null && [ $(( $(date +%s) - start )) -ge "$SAMPLE_SECONDS" ]; then
    line="RESULT peak_sent_s=$peak last_sent_s=$last"
    [ "$TELEMETRY_METRICS" = 1 ] && line="$line peak_flushed_s=$peak_flush peak_consumed_s=$peak_cons peak_dropped_s=$peak_drop"
    echo "$line"
    break
  fi
  sleep 5
done
