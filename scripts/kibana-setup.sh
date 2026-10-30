#!/bin/sh
# Creates the Kibana data views, then exits (run by the kibana-setup service).
#   App logs        -> app-logs-*                  (microservices: producer, consumer; filebeat-apps -> Kafka -> Logstash; the default)
#   Infra logs      -> infra-logs-*                (Kafka infrastructure: kafka-1, kafka-init, kafka-ui; filebeat -> Elasticsearch)
#   All logs        -> infra-logs-*,elk-logs-*     (every log except the microservices: infra + the ELK stack)
#   Kafka messages  -> kafka-messages-*            (app-events, app-log-messages, app-metrics, indexed by Logstash)
# Safe to run again: existing data views are overwritten.
set -eu

KIBANA_URL="${KIBANA_URL:-http://kibana:5601}"

echo "Waiting for Kibana at ${KIBANA_URL}..."
until curl -fs "${KIBANA_URL}/api/status" > /dev/null; do
  sleep 5
done

create_data_view() {
  id="$1"; name="$2"; pattern="$3"
  echo "Creating data view '${name}' (${pattern})"
  curl -fsS -X POST "${KIBANA_URL}/api/data_views/data_view" \
    -H "kbn-xsrf: true" -H "Content-Type: application/json" \
    -d "{\"override\": true, \"data_view\": {\"id\": \"${id}\", \"name\": \"${name}\", \"title\": \"${pattern}\", \"timeFieldName\": \"@timestamp\"}}" \
    > /dev/null
}

create_data_view app-logs "App logs" "app-logs-*"
create_data_view infra-logs "Infra logs" "infra-logs-*"
create_data_view all-logs "All logs" "infra-logs-*,elk-logs-*"
create_data_view kafka-messages "Kafka messages" "kafka-messages-*"

# Open Discover on the microservices' logs by default
curl -fsS -X POST "${KIBANA_URL}/api/data_views/default" \
  -H "kbn-xsrf: true" -H "Content-Type: application/json" \
  -d '{"data_view_id": "app-logs", "force": true}' > /dev/null

echo "Kibana is ready: http://localhost:5601/app/discover"
