#!/usr/bin/env bash
# MEASUREMENT-CAPACITY sweep: how much offered load can THIS configuration carry
# before it starts LOSING latency measurements? Unlike drain-scale-sweep.sh (which
# measures raw send-capacity against a drain, capture OFF), this drives the ECHO
# contestant (answers every order in microseconds, so the contestant is never the
# bottleneck and eBPF always has a response to stamp) with eBPF capture + telemetry
# ON, and steps the offered rate up on a SINGLE worker / SINGLE sandbox node (the
# current config). At each step it samples the four loss signals and the coverage
# ratio; the answer is the highest step that stays clean.
#
# A step is CLEAN when, in steady state:
#   - rate(iicpc_ebpf_ringbuf_dropped_total)  ~ 0   (kernel capture didn't overrun)
#   - rate(iicpc_ebpf_acked_dropped_total)    ~ 0   (publish queue didn't drop)
#   - rate(iicpc_bot_telemetry_events_dropped_total) ~ 0  (send-side telemetry)
#   - coverage = flushed/sent                 >= COV_MIN (default 0.99)
#   - consumed(orders.acked) ~ flushed        (ingester not falling behind)
#
# Usage:   deploy-bench/measure-capacity-sweep.sh ["50 100 150 200 250 300"]
# Env:     TAG (echo image tag, default v4) | DURATION_S (steady sample, 60)
#          WARMUP_S (deploy+barrier+ramp, 60) | SETTLE_S (between runs, 20)
#          COV_MIN (0.99) | PROM (http://localhost:9090)
# Prereqs: cluster up; echo image pushed at TAG; capture ON + worker telemetry ON +
#          KEDA off + 1 worker replica (e2e/02-bootstrap.sh leaves it this way);
#          Prometheus port-forward: kubectl -n observability port-forward svc/prometheus 9090:9090
set -euo pipefail
cd "$(dirname "$0")/.."
. e2e/lib.sh 2>/dev/null || { export AWS_PROFILE="${AWS_PROFILE:-iicpc}" AWS_REGION="${AWS_REGION:-us-east-1}"; }

RATES="${1:-50 100 150 200 250 300}"   # offered k-orders/s ladder (each = that many @1000/s tasks)
TAG="${TAG:-v4}"
DURATION_S="${DURATION_S:-60}"
WARMUP_S="${WARMUP_S:-60}"
SETTLE_S="${SETTLE_S:-20}"
COV_MIN="${COV_MIN:-0.99}"
PROM="${PROM:-http://localhost:9090}"
RESULTS="${RESULTS:-deploy-bench/measure-capacity-sweep.tsv}"
REG="${REG:-$(aws sts get-caller-identity --query Account --output text).dkr.ecr.${AWS_REGION}.amazonaws.com}"
KNS=data; KPOD=kafka-0; BROKER=localhost:9092

# ── prom instant-query helper -> float (0 on miss) ────────────────────────────
pq() {
  curl -sG "$PROM/api/v1/query" --data-urlencode "query=$1" \
    | python3 -c "import sys,json;r=json.load(sys.stdin)['data']['result'];print(float(r[0]['value'][1]) if r else 0.0)" 2>/dev/null || echo 0
}

# ── 1. register the echo contestant (capture runs because the orchestrator has
#       CAPTURE_ENABLED=true; echo replies to every order) ───────────────────────
ECHO_IMAGE="$REG/iicpc/contestant-echo:$TAG"
SUB="cap-echo-$(cat /proc/sys/kernel/random/uuid)"
echo ">> register echo submission $SUB -> $ECHO_IMAGE"
kubectl exec -i -n "$KNS" postgres-0 -- psql -U iicpc -d iicpc -v ON_ERROR_STOP=1 -c \
 "INSERT INTO submissions (submission_id,contestant_id,sha256,language,protocol,port,team_name,artifact_path,image_ref,status)
  VALUES ('$SUB','benchmark','$SUB','rust','FIX',9898,'cap-echo','n/a','$ECHO_IMAGE','ready');" >/dev/null

# ── 2. seed one all-new-limit constant scenario per rate (1:1 order:response so
#       coverage math is exact; no cancels/replaces to muddy the ratio) ──────────
TOTAL_S=$(( WARMUP_S + DURATION_S + 15 ))
python3 - "$TOTAL_S" $RATES <<'PY' | kubectl exec -i -n "$KNS" postgres-0 -- psql -U iicpc -d iicpc -v ON_ERROR_STOP=1 >/dev/null
import sys, json
total_s = int(sys.argv[1]); rates = [int(x) for x in sys.argv[2:]]
S = 10**9
print("BEGIN;")
for i, k in enumerate(rates):
    specs = [{"task_id": t, "profile": "hft", "target_rps": 1000,
              "start_offset_ns": 0, "duration_ns": total_s*S,
              "market_pct": 0, "cancel_pct": 0, "replace_pct": 0} for t in range(k)]
    js = json.dumps(specs).replace("'", "''")
    print(f"DELETE FROM scenarios WHERE name='cap-{k}k';")
    print(f"INSERT INTO scenarios (scenario_id,name,duration_ns,task_specs,sort_order) "
          f"VALUES ('cap-{k}k','cap-{k}k',{total_s*S},'{js}'::jsonb,{100+i});")
print("COMMIT;")
PY
echo ">> seeded scenarios: $(for k in $RATES; do printf 'cap-%sk ' "$k"; done)"

printf 'target_k\tsent_s\tdecoded_s\tflushed_s\tcoverage\trbuf_drop_s\tacked_drop_s\ttel_drop_s\tconsumed_acked_s\tverdict\n' | tee "$RESULTS"

best_clean=0
for k in $RATES; do
  echo; echo "##### offered ${k}k/s (scenario cap-${k}k) #####"
  SESSION="$(e2e_session_id 2>/dev/null || { ms=$(( $(date +%s%N)/1000000 )); ts=$(printf '%012x' "$ms"); rnd=$(cat /proc/sys/kernel/random/uuid|tr -d -); echo "${ts:0:8}-${ts:8:4}-7${rnd:1:3}-8${rnd:5:3}-${rnd:8:12}"; })"
  RGID=$(cat /proc/sys/kernel/random/uuid); NOW=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  MSG="{\"session_id\":\"$SESSION\",\"submission_id\":\"$SUB\",\"contestant_id\":\"benchmark\",\"run_group_id\":\"$RGID\",\"scenario_id\":\"cap-${k}k\",\"requested_at\":\"$NOW\"}"
  kubectl exec -n "$KNS" "$KPOD" -- bash -c \
    "echo '$RGID:$MSG' | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server $BROKER --topic benchmark.requested --property parse.key=true --property key.separator=:" \
    >/dev/null 2>&1 || true

  echo "   warmup ${WARMUP_S}s (deploy + barrier + ramp)…"; sleep "$WARMUP_S"

  # steady-state sample: track peak send and the MAX of every drop rate over the window
  echo "   sampling ${DURATION_S}s…"
  peak_sent=0; peak_dec=0; peak_flush=0; peak_cons=0; max_rbuf=0; max_akd=0; max_teld=0
  end=$(( $(date +%s) + DURATION_S ))
  while [ "$(date +%s)" -lt "$end" ]; do
    sent=$(pq 'sum(rate(iicpc_bot_orders_sent_total[30s]))')
    dec=$(pq  'sum(rate(iicpc_ebpf_events_decoded_total[30s]))')
    flu=$(pq  'sum(rate(iicpc_ebpf_events_flushed_total[30s]))')
    rbuf=$(pq 'sum(rate(iicpc_ebpf_ringbuf_dropped_total[30s]))')
    akd=$(pq  'sum(rate(iicpc_ebpf_acked_dropped_total[30s]))')
    teld=$(pq 'sum(rate(iicpc_bot_telemetry_events_dropped_total[30s]))')
    cons=$(pq 'sum(rate(iicpc_telemetry_events_consumed_total{topic="orders.acked"}[30s]))')
    awk_max() { awk -v a="$1" -v b="$2" 'BEGIN{print (a>b)?a:b}'; }
    peak_sent=$(awk_max "$peak_sent" "$sent"); peak_dec=$(awk_max "$peak_dec" "$dec")
    peak_flush=$(awk_max "$peak_flush" "$flu"); peak_cons=$(awk_max "$peak_cons" "$cons")
    max_rbuf=$(awk_max "$max_rbuf" "$rbuf"); max_akd=$(awk_max "$max_akd" "$akd"); max_teld=$(awk_max "$max_teld" "$teld")
    printf "   %s sent/s=%.0f dec/s=%.0f flush/s=%.0f cons/s=%.0f | rbufDrop=%.0f akDrop=%.0f telDrop=%.0f\n" \
      "$(date +%H:%M:%S)" "$sent" "$dec" "$flu" "$cons" "$rbuf" "$akd" "$teld"
    sleep 5
  done

  # Verdict is DROP-based: a step is CLEAN when the measurement pipeline drops ~no
  # samples (ringbuf/acked/telemetry). Coverage (flushed/sent) is reported for context
  # but NOT gated on, because at high rate it falls due to echo TIMING OUT orders
  # (contestant capacity, cross-node) — that's not measurement loss. DROP_THRESH/s
  # tolerates scrape noise.
  # STALL guard: a run that collapsed (worker wedged / load didn't sustain) has no
  # real sustained load, so its 0 drops are meaningless — mark it STALL, not CLEAN.
  # Require both sent and flushed to reach a fraction of the offered target.
  read -r cov verdict < <(awk -v s="$peak_sent" -v f="$peak_flush" -v rb="$max_rbuf" -v ak="$max_akd" -v td="$max_teld" -v dt="${DROP_THRESH:-50}" -v tgt="$((k*1000))" 'BEGIN{
    c=(s>0)?f/s:0;
    if (s < 0.5*tgt || f < 0.4*tgt) v="STALL";
    else if (rb<dt && ak<dt && td<dt) v="CLEAN";
    else v="LOSSY";
    printf "%.4f %s\n", c, v }') || true
  printf '%s\t%.0f\t%.0f\t%.0f\t%s\t%.0f\t%.0f\t%.0f\t%.0f\t%s\n' \
    "$k" "$peak_sent" "$peak_dec" "$peak_flush" "$cov" "$max_rbuf" "$max_akd" "$max_teld" "$peak_cons" "$verdict" | tee -a "$RESULTS"
  if [ "$verdict" = CLEAN ]; then best_clean="$k"; fi

  echo "   settle ${SETTLE_S}s…"; sleep "$SETTLE_S"
done

echo; echo "================================================================"
echo ">> highest CLEAN offered rate (no measurement loss): ${best_clean}k/s"
echo ">> full table: $RESULTS"
echo "   (LOSSY at the next step up = that's the measurement ceiling for this config)"
