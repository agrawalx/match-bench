#!/usr/bin/env bash
# Build + push the pure-drain TCP sink that stands in for the contestant during a
# load-generator capacity run. It read-and-discards (never replies), so it never
# back-pressures the bots — iicpc_bot_orders_sent then reflects the load gen's
# real push ceiling, not contestant app logic. Binds 9898 (eBPF-captured +
# orchestrator readiness port).
#
#   DRAIN_IMAGE=<ref> deploy-bench/build-drain.sh
# Local k3s default pushes via localhost:5000 (docker treats localhost insecure);
# the node pulls the same repo via the 192.168.1.11:5000 mirror. For EKS set
# DRAIN_IMAGE to the ECR ref and push with your usual ECR login.
set -euo pipefail
cd "$(dirname "$0")/.."
DRAIN_IMAGE="${DRAIN_IMAGE:-192.168.1.11:5000/iicpc/contestant-drain:bench}"

echo ">> building $DRAIN_IMAGE"
docker build -f services/bot-fleet/contestant-drain.Dockerfile -t "$DRAIN_IMAGE" .

# Local registry: push via localhost alias (insecure-by-default for the daemon).
PUSH_REF="$DRAIN_IMAGE"
case "$DRAIN_IMAGE" in
  192.168.1.11:5000/*)
    PUSH_REF="localhost:5000/${DRAIN_IMAGE#*/}"
    docker tag "$DRAIN_IMAGE" "$PUSH_REF"
    ;;
esac
echo ">> pushing $PUSH_REF"
docker push "$PUSH_REF"
echo ">> done. Use this as DRAIN_IMAGE for seed-drain-submission.sh: $DRAIN_IMAGE"
