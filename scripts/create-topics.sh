#!/bin/bash
# Creates the project topics. Safe to re-run: existing topics are left alone.
set -euo pipefail

KAFKA_BIN=/opt/kafka/bin
BOOTSTRAP="${BOOTSTRAP_SERVERS:-kafka-1:9092}"

# name:partitions:replication-factor (1: the lab runs a single broker)
TOPICS=(
  "app-logs:3:1"           # the apps' own log lines (ECS JSON), shipped by filebeat-apps
  "app-log-messages:3:1"   # LogMessage records from the producer's POST /api/logs
  "app-metrics:3:1"
  "app-events:3:1"
)

echo "Waiting for the cluster at ${BOOTSTRAP}..."
until "$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --list > /dev/null 2>&1; do
  sleep 2
done

for entry in "${TOPICS[@]}"; do
  IFS=":" read -r name partitions rf <<< "$entry"
  echo "Creating topic '${name}' (partitions=${partitions}, replication-factor=${rf})"
  "$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" \
    --create --if-not-exists \
    --topic "$name" \
    --partitions "$partitions" \
    --replication-factor "$rf" \
    --config retention.ms=604800000
done

echo "Topics in the cluster:"
"$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --describe
