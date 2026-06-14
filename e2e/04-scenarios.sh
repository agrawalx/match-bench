#!/usr/bin/env bash
# Seed the e2e DEFAULT scenarios: constant / spike / ramp, with a churning order mix
# (market 10 / cancel 30 / replace 10 / limit 50) so the reference order book stays
# bounded (cancels remove resting orders; crossing prices from order_shape match) —
# this keeps both the matching-engine contestant and the validator within memory.
#
#   constant : 60s  @ 50k/s              -> 50 tasks @1000/s, all start at t=0
#   spike    : 40/20/40 @ 100k peak      -> 50 baseline (whole run) + 50 burst (t=40..60s)
#   ramp     : linear 20k -> 100k in 120s-> 20 baseline + 80 staggered over 118s
#
# These peaks (<=100k/s) sit UNDER the delivered ceiling so no acks are dropped
# (clean correctness), and the order volumes are small enough for the matching engine
# (ALGO_MEMORY=4Gi) and the validator (8Gi) to handle -> a QUALIFIED run. To stress
# throughput, scale the rates up + use the echo contestant with telemetry OFF (the
# matching engine + validator won't keep up past the ceiling — see README).
#
# Per-task rate is fixed at 1000/s; aggregate scales by task count + start_offset
# staggering. Usage: e2e/04-scenarios.sh
set -euo pipefail
PGNS=data; PGPOD=postgres-0; PGUSER=iicpc; PGDB=iicpc

# Generate the SQL directly to a file (the ramp scenario has 500 tasks — too large to
# pass through a shell argv, so build + emit it all in one Python pass).
python3 - <<'PY' > /tmp/e2e_scenarios.sql
import json
S = 10**9
RPS = 1000
mix = {"market_pct": 10, "cancel_pct": 30, "replace_pct": 10}  # limit = remainder = 50

def task(i, off_ns, dur_ns):
    return {"task_id": i, "profile": "hft", "target_rps": RPS,
            "start_offset_ns": off_ns, "duration_ns": dur_ns, **mix}

constant = [task(i, 0, 60 * S) for i in range(50)]                                    # 50k flat, 60s
spike = [task(i, 0, 100 * S) for i in range(50)] + [task(50 + i, 40 * S, 20 * S) for i in range(50)]  # 50k ->100k(40-60s)->50k
ramp = [task(i, 0, 120 * S) for i in range(20)]                                       # 20k baseline
for k in range(80):                                                                  # ramp to 100k over 118s
    off = round(k / 79 * 118 * S)
    ramp.append(task(20 + k, off, 120 * S - off))

scenarios = {"constant": (60 * S, constant, 1), "spike": (100 * S, spike, 2), "ramp": (120 * S, ramp, 3)}
print("BEGIN;")
# Replace any existing constant/spike/ramp (the default seed uses these names with a
# unique-name constraint), keeping other scenarios (hft-load, etc.) untouched.
print("DELETE FROM scenarios WHERE name IN ('constant','spike','ramp');")
for name, (dur, specs, order) in scenarios.items():
    js = json.dumps(specs).replace("'", "''")
    print(f"INSERT INTO scenarios (scenario_id, name, duration_ns, task_specs, sort_order)")
    print(f"VALUES ('{name}','{name}',{dur},'{js}'::jsonb,{order});")
print("SELECT name, jsonb_array_length(task_specs) AS tasks, (task_specs->0->>'cancel_pct') AS cancel_pct, duration_ns/1000000000 AS dur_s FROM scenarios WHERE name IN ('constant','spike','ramp') ORDER BY sort_order;")
print("COMMIT;")
PY

echo ">> seeding constant / spike / ramp (churn mix: market10/cancel30/replace10/limit50)"
kubectl exec -i -n "$PGNS" "$PGPOD" -- psql -U "$PGUSER" -d "$PGDB" -v ON_ERROR_STOP=1 < /tmp/e2e_scenarios.sql 2>&1 | grep -vE "Defaulted"
echo ">> done. (RESEED_SCENARIOS should be false so a submission-api restart doesn't re-add defaults)"
