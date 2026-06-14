#!/usr/bin/env bash
# Horizontal-scaling capacity sweep for the bot-fleet load generators against a
# DRAIN sink. Two modes via the TELEMETRY env var:
#
#   TELEMETRY=off (default) — telemetry DISABLED (BOT_DISABLE_TELEMETRY=1). Measures
#     PURE send-capacity (orders sent/s): how fast the load gens generate, isolated
#     from the telemetry/Kafka path. Workers are independent, so this should scale
#     ~linearly 1 -> N nodes.
#
#   TELEMETRY=on            — telemetry ENABLED. Measures the TELEMETRY PIPELINE:
#     events flushed/s (worker -> Kafka) and consumed/s (by the ingester). Caveat:
#     against a drain, every order ALSO evicts at RESPONSE_TIMEOUT, so the watchdog
#     emits a 2nd telemetry event/order (worker.rs:1029) — ~2x telemetry load and
#     the exact storm that OOM'd before. It is now bounded (BOT_MAX_INFLIGHT_PER_TASK
#     cap + 6Gi limit + lossless record() that BLOCKS, not grows), so send
#     self-throttles to the telemetry-deliverable rate instead of crashing. The
#     scaling question: if flushed/consumed does NOT ~double 1 -> 2 nodes, the
#     SINGLE broker is the cap -> that's the data justifying the 2-broker path.
#     WATCH `kubectl -n benchmark get pods -l app=bot-fleet-worker` for restarts;
#     if any OOMKill, lower BOT_MAX_INFLIGHT_PER_TASK and re-run.
#
# Per sweep point N it: waits for N Ready botworker nodes (prints the terraform
# command + polls — it NEVER runs terraform; node count stays yours), seeds an
# N*1000-HFT-task scenario (-> worker_count = N), pins N worker pods one-per-node,
# triggers a run, samples for DURATION_S+30s, and records to a TSV.
#
#   TELEMETRY=on DRAIN_IMAGE=<ecr-ref> deploy-bench/drain-scale-sweep.sh "1 2"
#
# Prereqs: cluster up (deploy-bench/eks-up.sh), drain image in ECR
# (deploy-bench/build-drain.sh), Prometheus at $PROM (kubectl -n observability
# port-forward svc/prometheus 9090:9090), botworker_max_size >= max(POINTS).
set -euo pipefail
cd "$(dirname "$0")/.."

POINTS="${1:-1 2}"                          # node counts to sweep, in order
TELEMETRY="${TELEMETRY:-off}"               # off = send-capacity | on = telemetry pipeline
DURATION_S="${DURATION_S:-120}"             # scenario length per point
PROM="${PROM:-http://localhost:9090}"
RESULTS="${RESULTS:-deploy-bench/scale-sweep-${TELEMETRY}.tsv}"
K=kubectl

ready_botworkers() {
  ${K} get nodes -l pool=botworker \
    -o jsonpath='{range .items[*]}{range @.status.conditions[?(@.type=="Ready")]}{.status}{"\n"}{end}{end}' \
    2>/dev/null | grep -c True || true
}
# getv <key> — pull an integer "<key>=N" out of the captured run-sentps output.
getv() { printf '%s\n' "$out" | sed -n "s/.*$1=\\([0-9]*\\).*/\\1/p" | tail -1; }

echo "##### register drain submission (capture off, ALGO_CPU=2, reseed off) #####"
SUB=$(bash deploy-bench/seed-drain-submission.sh | sed -n 's/^SUBMISSION_ID=//p' | tail -1)
[ -n "$SUB" ] || { echo "ERROR: could not register drain submission" >&2; exit 1; }
echo ">> drain submission_id=$SUB"

if [ "$TELEMETRY" = on ]; then
  echo "##### telemetry ON — measuring the telemetry pipeline (flushed/consumed) #####"
  ${K} -n benchmark set env deployment/bot-fleet-worker BOT_DISABLE_TELEMETRY- >/dev/null 2>&1 || true
  TELEMETRY_METRICS=1
  printf 'nodes\tpeak_sent_s\tpeak_flushed_s\tpeak_consumed_s\tper_node_consumed\n' | tee "$RESULTS"
else
  echo "##### telemetry OFF — measuring pure send-capacity (orders sent/s) #####"
  ${K} -n benchmark set env deployment/bot-fleet-worker BOT_DISABLE_TELEMETRY=1 >/dev/null
  TELEMETRY_METRICS=0
  printf 'nodes\tpeak_sent_s\tper_node\n' | tee "$RESULTS"
fi

for N in $POINTS; do
  echo
  echo "##### sweep point: $N botworker node(s) #####"

  while [ "$(ready_botworkers)" -lt "$N" ]; do
    echo "   need $N Ready botworker node(s), have $(ready_botworkers) — run this, then I'll continue:"
    echo "     (cd infra/terraform && terraform apply -var botworker_desired_size=$N)"
    sleep 20
  done
  echo ">> $N botworker node(s) Ready"

  bash deploy-bench/seed-hft-load.sh "$N" "$DURATION_S"   # N*1000 HFT tasks @1000/s
  bash deploy-bench/bench-patches.sh "$N"                 # N worker pods, one per node

  out=$(SAMPLE_SECONDS=$((DURATION_S + 30)) PROM="$PROM" TELEMETRY_METRICS="$TELEMETRY_METRICS" \
        bash deploy-bench/run-sentps.sh "$SUB" hft-load)
  echo "$out"

  sent=$(getv peak_sent_s); sent=${sent:-0}
  if [ "$TELEMETRY" = on ]; then
    flush=$(getv peak_flushed_s); flush=${flush:-0}
    cons=$(getv peak_consumed_s); cons=${cons:-0}
    perc=0; [ "$N" -gt 0 ] && perc=$(( cons / N ))
    printf '%s\t%s\t%s\t%s\t%s\n' "$N" "$sent" "$flush" "$cons" "$perc" | tee -a "$RESULTS"
  else
    per=0; [ "$N" -gt 0 ] && per=$(( sent / N ))
    printf '%s\t%s\t%s\n' "$N" "$sent" "$per" | tee -a "$RESULTS"
  fi
done

echo
echo "##### SCALING SUMMARY (drain sink, telemetry ${TELEMETRY}) #####"
column -t "$RESULTS"
# Scale on the mode's headline column: consumed/s (col 4, telemetry on) or sent/s (col 2).
col=$([ "$TELEMETRY" = on ] && echo 4 || echo 2)
awk -F'\t' -v c="$col" 'NR==2{base=$c} NR>1 && base>0 {
  printf "  %s node(s): %d %s  (%.2fx vs 1 node, %.0f%% per-node)\n",
         $1, $c, (c==4?"consumed/s":"sent/s"), $c/base, ($c/base)/$1*100 }' "$RESULTS"

if [ "$TELEMETRY" = off ]; then
  echo
  echo ">> re-enable telemetry for normal (acked/latency) runs:"
  echo "     ${K} -n benchmark set env deploy/bot-fleet-worker BOT_DISABLE_TELEMETRY-"
fi
