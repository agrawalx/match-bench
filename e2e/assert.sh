#!/usr/bin/env bash
# Gate a completed run: every service survived, latency rows exist, correctness ran,
# zero drops, no OOM. Exits non-zero if any check fails. Usage: e2e/assert.sh [session-id]
set -euo pipefail
. "$(dirname "$0")/lib.sh"
SESSION="${1:-$(cat "$REPO_ROOT/e2e/.last_session" 2>/dev/null)}"
: "${SESSION:?provide a session id (or run run.sh first)}"
fail=0
say() { printf '%-42s %s\n' "$1" "$2"; }

echo "== asserting session $SESSION =="

# 1. no pod restarts (OOM/crash) anywhere in the data path
for ns in benchmark sandbox data; do
  bad=$(kubectl -n "$ns" get pods --no-headers 2>/dev/null \
    | awk '$4+0 > 0 {print $1"("$4")"}' | tr '\n' ' ')
  if [ -n "$bad" ]; then say "[$ns] pods with restarts:" "$bad  ✗"; fail=1; else say "[$ns] no pod restarts" "✓"; fi
done

# 2. zero telemetry drops (worker counter) — needs a prometheus/metrics read; use the store gate below as primary
drops=$(kubectl -n benchmark logs -l app=bot-fleet-worker --tail=2000 2>/dev/null \
  | grep -c "telemetry channel closed" || true)
if [ "${drops:-0}" -gt 0 ]; then say "telemetry drops (log)" "$drops  ✗"; fail=1; else say "no telemetry-drop logs" "✓"; fi

# 3. latency rows present for the session (ingester wrote service_time-bearing metrics)
rows=$(psql_exec "SELECT count(*) FROM metrics WHERE session_id='$SESSION';" | grep -oE '[0-9]+' | head -1 || echo 0)
if [ "${rows:-0}" -gt 0 ]; then say "latency rows (metrics table)" "$rows  ✓"; else say "latency rows" "0  ✗ (eBPF/service_time missing?)"; fail=1; fi

# 4. percentiles (service_time / response_time) for a quick eyeball
psql_exec "SELECT wave_index,
                  round(p50_ns/1000.0,1)  AS svc_p50_us,
                  round(p99_ns/1000.0,1)  AS svc_p99_us,
                  round(p999_ns/1000.0,1) AS svc_p999_us
           FROM metrics WHERE session_id='$SESSION' ORDER BY wave_index LIMIT 10;" || true

# 5. correctness validation ran + a score exists
corr=$(psql_exec "SELECT count(*) FROM correctness_summary WHERE session_id='$SESSION';" | grep -oE '[0-9]+' | head -1 || echo 0)
if [ "${corr:-0}" -gt 0 ]; then say "correctness summary present" "✓"; else say "correctness summary" "missing  ✗ (validator OOM/timeout?)"; fail=1; fi

echo "== $([ $fail -eq 0 ] && echo PASS || echo FAIL) =="
exit $fail
