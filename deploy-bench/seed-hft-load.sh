#!/usr/bin/env bash
# Seed a PURE-HFT throughput scenario for the load-gen scaling sweep.
#
# Every task is an HFT bot firing EXACTLY 1000 orders/s — the per-task tokio
# ceiling (a single task can't reliably exceed ~1000/s because tokio's timer
# wheel quantises to 1ms; catch-up pacing is what lets it hold the wall). So you
# scale throughput by ADDING tasks, never by faster tasks:
#
#   N nodes  ->  N*1000 HFT tasks  ->  controller worker_count = ceil(N*1000/1000)
#            ->  N workers (one per botworker node)  ->  target = N million orders/s
#
# Pure NewOrderSingle (no cancel/replace) so every order is one fresh send — the
# densest possible firehose. Replaces ALL seeded scenarios so the benchmark runs
# ONLY this one.
#
# Usage (pair all three per sweep point):
#   cd infra/terraform && terraform apply -var botworker_desired_size=N
#   deploy-bench/seed-hft-load.sh N [duration_s]   # N*1000 tasks, target N M/s
#   deploy-bench/bench-patches.sh N                # pin N workers, one per node
set -euo pipefail
NODES="${1:?usage: seed-hft-load.sh <node_count> [duration_s]}"
DURATION_S="${2:-60}"          # keep modest: a 1M/s x 60s session is 60M orders;
                               # longer drowns the correctness-validator drain.
# HFT task count drives the headline rps (1000/s each). Defaults to NODES*1000
# for the N-million/s sweep; override HFT_TASKS for sub-million targets, e.g.
# HFT_TASKS=500 -> 500k/s on a single node.
HFT_TASKS="${HFT_TASKS:-$(( NODES * 1000 ))}"
# Optional realistic-mix layers, OFF by default so the sweep stays pure-HFT.
# Rates + order-type mix mirror submission-api scenarios/builder.go and
# cmd/loadgen-seed. They add little rps but exercise the market + cancel/replace
# paths without the 5-orders/s retail profile exploding the task count.
RETAIL="${RETAIL:-0}"          # retail tasks @5/s   (market 65 / cancel 5)
INST="${INST:-0}"              # institutional @300/s (market 20)
DUR_NS=$(( DURATION_S * 1000000000 ))
PGNS=data; PGPOD=postgres-0; PGUSER=iicpc; PGDB=iicpc

echo ">> generating ${HFT_TASKS} HFT @1000/s + ${RETAIL} retail @5/s + ${INST} institutional @300/s  (${DURATION_S}s)"
JSON=$(python3 - "$HFT_TASKS" "$RETAIL" "$INST" "$DUR_NS" <<'PY'
import json, sys
hft, retail, inst, dur = (int(x) for x in sys.argv[1:5])
tasks, tid = [], 0
def layer(n, profile, rps, market, cancel, replace):
    global tid
    for _ in range(n):
        tasks.append({
            "task_id": tid, "profile": profile, "target_rps": rps,
            "start_offset_ns": 0, "duration_ns": dur,
            "market_pct": market, "cancel_pct": cancel, "replace_pct": replace,
        })
        tid += 1
layer(hft,    "hft",           1000,  0, 0, 0)   # pure new-limit firehose
layer(retail, "retail",           5, 65, 5, 0)
layer(inst,   "institutional", 300, 20, 0, 0)
print(json.dumps(tasks))
PY
)

echo ">> replacing scenarios with single 'hft-load' (DELETE defaults + insert)"
kubectl exec -i -n "$PGNS" "$PGPOD" -- psql -U "$PGUSER" -d "$PGDB" -v ON_ERROR_STOP=1 <<SQL
DELETE FROM scenarios;
INSERT INTO scenarios (scenario_id, name, duration_ns, task_specs, sort_order)
VALUES ('hft-load', 'hft-load', ${DUR_NS}, '${JSON}'::jsonb, 1);
SELECT name, jsonb_array_length(task_specs) AS tasks, duration_ns/1000000000 AS dur_s FROM scenarios;
SQL

cat <<EOF

>> seeded. NOTE:
   - Set submission-api RESEED_SCENARIOS=false (else a restart re-adds the 3
     defaults and the benchmark would run them too):
       kubectl -n platform set env deploy/submission-api RESEED_SCENARIOS=false
   - MAX_TASKS_PER_WORKER (controller) and MAX_BOTS_PER_WORKER (worker) must both
     be 1000 so worker_count == ${NODES} == botworker node count.
   - Trigger a benchmark, then read where it caps:
       * bot-fleet "orders sent /s"      -> offered load (does it reach ${NODES}M?)
       * telemetry-ingester consumed /s  -> delivered/acked throughput
       * per-task: offered / ${HFT_TASKS}    -> is each HFT task holding ~1000/s?
     The ceiling is whichever pins first: load-gen node CPU, the contestant's
     2-core algo (ALGO_CPU=2), or Kafka disk/lag.
EOF
