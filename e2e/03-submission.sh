#!/usr/bin/env bash
# Register the ECHO contestant (pre-built image) as a ready submission for the
# TERMINAL run flow (run.sh). The echo just ACKs every order — it's a THROUGHPUT
# contestant and will be DISQUALIFIED on correctness (no order book).
#
# For a QUALIFIED run, use the FRONTEND flow instead: port-forward svc/frontend and
# upload e2e/contestant-matching-engine.zip (a correct price-time order book) — no
# sign-in (auth is removed). The build-worker compiles it to an image, then run it.
# (e2e/contestant-fix-acker.zip is another throughput contestant.)
#
# Writes the submission id to e2e/.submission_id for run.sh. After 02-bootstrap.sh.
# Requires TAG.
set -euo pipefail
. "$(dirname "$0")/lib.sh"
: "${TAG:?set TAG to the image tag from 01-images.sh}"
ECHO_IMAGE="$REG/iicpc/contestant-echo:$TAG"
SUB="echo-$(cat /proc/sys/kernel/random/uuid)"

echo ">> registering echo submission $SUB -> $ECHO_IMAGE (FIX :9898, ready, capture ON)"
psql_exec "INSERT INTO submissions
  (submission_id, contestant_id, sha256, language, protocol, port, team_name, artifact_path, image_ref, status)
 VALUES
  ('$SUB','echo-contestant','e2e','rust','FIX',9898,'e2e-echo','n/a','$ECHO_IMAGE','ready');"

echo "$SUB" > "$REPO_ROOT/e2e/.submission_id"
echo ">> submission id saved to e2e/.submission_id"
