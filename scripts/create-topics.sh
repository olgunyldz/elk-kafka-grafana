#!/bin/bash
# Creates the project topics, and raises the replication factor of topics that have fewer copies than
# wanted (for example after going from one broker to two). Safe to re-run: topics already as wanted
# are left alone.
set -euo pipefail

KAFKA_BIN=/opt/kafka/bin
BOOTSTRAP="${BOOTSTRAP_SERVERS:-kafka-1:9092}"
BROKER_IDS=(1 2)

# name:partitions:replication-factor:topic configs (2: a copy on each broker)
TOPICS=(
  "app-logs:3:2:retention.ms=604800000"           # the apps' own log lines (ECS JSON), shipped by filebeat-apps
  "app-log-messages:3:2:retention.ms=604800000"   # LogMessage records from the producer's POST /api/logs
  "app-metrics:3:2:retention.ms=604800000"
  "app-events:3:2:retention.ms=604800000"
  # Audit records (regulatory log): kept forever (the archive and Elasticsearch apply the retention
  # periods), never compacted (compaction would delete records), and written only when both brokers
  # have them (min.insync.replicas=2: with a broker down, audited actions are refused)
  "audit-events:3:2:retention.ms=-1,cleanup.policy=delete,min.insync.replicas=2"
)
# Kafka's own topic for consumer group offsets: created by the broker, but its copies count too
INTERNAL_RF=2

echo "Waiting for the cluster at ${BOOTSTRAP}..."
until "$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --list > /dev/null 2>&1; do
  sleep 2
done

# Gives every partition of a topic that has fewer than $2 replicas a copy on the other brokers too.
# The current replicas stay first, so the leader doesn't change.
ensure_replication() {
  local topic="$1" rf="$2" parts=() json=""
  while read -r line; do
    local partition replicas
    partition=$(sed -E 's/.*Partition: ([0-9]+).*/\1/' <<< "$line")
    replicas=$(sed -E 's/.*Replicas: ([0-9,]+).*/\1/' <<< "$line")
    IFS="," read -r -a current <<< "$replicas"
    (( ${#current[@]} >= rf )) && continue
    local wanted=("${current[@]}")
    for id in "${BROKER_IDS[@]}"; do
      (( ${#wanted[@]} >= rf )) && break
      [[ " ${wanted[*]} " == *" $id "* ]] || wanted+=("$id")
    done
    parts+=("{\"topic\":\"$topic\",\"partition\":$partition,\"replicas\":[$(IFS=,; echo "${wanted[*]}")]}")
  done < <("$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --describe --topic "$topic" | grep "Partition:")

  if (( ${#parts[@]} == 0 )); then
    return
  fi
  json="{\"version\":1,\"partitions\":[$(IFS=,; echo "${parts[*]}")]}"
  echo "Raising '${topic}' to replication factor ${rf} (${#parts[@]} partitions)"
  echo "$json" > /tmp/reassign-"$topic".json
  "$KAFKA_BIN/kafka-reassign-partitions.sh" --bootstrap-server "$BOOTSTRAP" \
    --reassignment-json-file /tmp/reassign-"$topic".json --execute > /dev/null
  until "$KAFKA_BIN/kafka-reassign-partitions.sh" --bootstrap-server "$BOOTSTRAP" \
      --reassignment-json-file /tmp/reassign-"$topic".json --verify | grep -q "^Reassignment of partition.*is complete" \
      && ! "$KAFKA_BIN/kafka-reassign-partitions.sh" --bootstrap-server "$BOOTSTRAP" \
      --reassignment-json-file /tmp/reassign-"$topic".json --verify | grep -q "still in progress"; do
    sleep 1
  done
}

for entry in "${TOPICS[@]}"; do
  IFS=":" read -r name partitions rf configs <<< "$entry"
  config_args=()
  IFS="," read -r -a config_list <<< "$configs"
  for c in "${config_list[@]}"; do config_args+=(--config "$c"); done
  echo "Creating topic '${name}' (partitions=${partitions}, replication-factor=${rf}, ${configs})"
  "$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" \
    --create --if-not-exists \
    --topic "$name" \
    --partitions "$partitions" \
    --replication-factor "$rf" \
    "${config_args[@]}"
  ensure_replication "$name" "$rf"
  # An existing topic keeps its old configs on --create --if-not-exists: set them explicitly
  "$KAFKA_BIN/kafka-configs.sh" --bootstrap-server "$BOOTSTRAP" --alter --entity-type topics \
    --entity-name "$name" --add-config "$configs" > /dev/null
done

if "$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --list | grep -qx "__consumer_offsets"; then
  ensure_replication "__consumer_offsets" "$INTERNAL_RF"
fi

echo "Topics in the cluster:"
"$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --describe --exclude-internal
