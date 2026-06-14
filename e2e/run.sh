#!/usr/bin/env bash
# Trigger one scenario against the echo contestant, wait for the run to complete,
# print throughput + the session id (feed it to assert.sh). Usage: e2e/run.sh <constant|spike|ramp>
set -euo pipefail
. "$(dirname "$0")/lib.sh"
SCENARIO="${1:?usage: run.sh <constant|spike|ramp>}"
SUB="$(cat "$REPO_ROOT/e2e/.submission_id")"
SESSION="$(new_session_id)"
RG="$(cat /proc/sys/kernel/random/uuid)"
NOW="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

MSG="{\"session_id\":\"$SESSION\",\"submission_id\":\"$SUB\",\"contestant_id\":\"echo-contestant\",\"run_group_id\":\"$RG\",\"scenario_id\":\"$SCENARIO\",\"requested_at\":\"$NOW\"}"
echo ">> triggering scenario=$SCENARIO submission=$SUB session=$SESSION"
kafka_produce benchmark.requested "$RG" "$MSG"

echo ">> waiting for run to complete (controller verdict)..."
deadline=$(( $(date +%s) + 360 ))
while [ "$(date +%s)" -lt "$deadline" ]; do
  sleep 5
  line=$(kubectl -n benchmark logs -l app=bot-fleet-controller --tail=200 2>/dev/null \
    | grep "$SESSION" | grep -oE '"status":"(completed|failed)"' | tail -1 || true)
  [ -n "$line" ] && { echo ">> controller: $line"; break; }
done

sent=$(kubectl -n benchmark logs -l app=bot-fleet-worker --tail=400 2>/dev/null \
  | grep "$SESSION" | grep -oE '"sent":[0-9]+' | awk -F: '{s+=$2} END{print s}')
echo ">> total sent (all workers): ${sent:-unknown}"
echo "$SESSION" > "$REPO_ROOT/e2e/.last_session"
echo ">> session id: $SESSION   (run: e2e/assert.sh $SESSION)"
