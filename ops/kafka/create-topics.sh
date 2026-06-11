#!/usr/bin/env bash
# ops/kafka/create-topics.sh
#
# This script configures operational tooling for create topics.
# It belongs to the IICPC operational toolchain and should keep
# setup, validation, and deployment behavior explicit at entry points.
# Function-level comments describe reusable shell routines below.

set -euo pipefail

bootstrap_server="${KAFKA_BOOTSTRAP_SERVER:-kafka-1:9092}"
topic_config=(
  --config min.insync.replicas=2
  --config max.message.bytes=1048576
)

# create_topic performs the script-specific operation described by its name.
# It keeps command side effects explicit and returns shell status to callers.
create_topic() {
  local name="$1"
  local partitions="$2"
  local retention_ms="$3"

  /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$bootstrap_server" \
    --create \
    --if-not-exists \
    --topic "$name" \
    --partitions "$partitions" \
    --replication-factor 3 \
    "${topic_config[@]}" \
    --config "retention.ms=$retention_ms"
}

create_topic submission.build.requested 3 604800000
create_topic submission.status.updated 3 604800000
create_topic benchmark.requested 3 604800000
create_topic benchmark.status.updated 3 604800000
create_topic workload.assignments 24 86400000
create_topic barrier 3 86400000
create_topic bot.ready 3 86400000
create_topic workload.failed 3 604800000
create_topic orders.sent 24 86400000
create_topic orders.acked 24 86400000
create_topic scores.correctness 3 2592000000
create_topic leaderboard.updates 3 604800000
