#!/bin/sh
# Creates the Kibana data views, then exits (run by the kibana-setup service).
#   App logs        -> app-logs-*                  (microservices: producer, consumer, message-api, audit-verifier;
#                                                   filebeat-apps -> Kafka -> Logstash; the default)
#   Infra logs      -> infra-logs-*                (Kafka infrastructure: kafka-1, kafka-2, kafka-init, kafka-ui; filebeat -> Elasticsearch)
#   All logs        -> infra-logs-*,elk-logs-*     (every log except the microservices: infra + the ELK stack)
#   Kafka messages  -> kafka-messages-*            (app-events, app-log-messages, app-metrics, indexed by Logstash)
#   Audit events    -> audit-*                     (regulation records from audit-events, step 09)
# Plus, for the audit indices: the retention policy (ILM), the index template, and the saved
# searches auditors ask for. Then the dashboards "Microservices" and "Audit log".
# Safe to run again: everything is overwritten.
set -eu

KIBANA_URL="${KIBANA_URL:-http://kibana:5601}"
ES_URL="${ES_URL:-http://elasticsearch:9200}"

# ---- Audit indices: retention and mapping (Elasticsearch) --------------------------------------
# audit-YYYY.MM, one index per month. Hot (searchable at full speed) for 3 months (PCI DSS 10.5.1:
# the last 3 months immediately available), then read-only and merged, deleted after 10 years
# (TTK 10 years; MASAK 8 years; PCI DSS 12 months). This is the search copy: the archive files are
# the record of truth and are kept just as long, on WORM storage in production.
echo "Creating the ILM policy audit-retention and the index template audit"
curl -fsS -X PUT "${ES_URL}/_ilm/policy/audit-retention" -H "Content-Type: application/json" -d '{
  "policy": {
    "phases": {
      "hot":    { "actions": {} },
      "warm":   { "min_age": "90d",   "actions": { "readonly": {}, "forcemerge": { "max_num_segments": 1 } } },
      "delete": { "min_age": "3650d", "actions": { "delete": {} } }
    }
  }
}' > /dev/null
curl -fsS -X PUT "${ES_URL}/_index_template/audit" -H "Content-Type: application/json" -d '{
  "index_patterns": ["audit-*"],
  "priority": 200,
  "template": {
    "settings": {
      "number_of_shards": 1,
      "number_of_replicas": 0,
      "index.lifecycle.name": "audit-retention"
    },
    "mappings": {
      "dynamic_templates": [
        { "details": { "path_match": "details.*", "mapping": { "type": "keyword" } } }
      ],
      "properties": {
        "@timestamp": { "type": "date" },
        "timestamp":  { "type": "date" },
        "eventId":    { "type": "keyword" },
        "category":   { "type": "keyword" },
        "action":     { "type": "keyword" },
        "channel":    { "type": "keyword" },
        "actor":      { "properties": { "type": { "type": "keyword" }, "id": { "type": "keyword" }, "role": { "type": "keyword" } } },
        "source":     { "properties": { "ip": { "type": "ip" }, "deviceId": { "type": "keyword" }, "appVersion": { "type": "keyword" } } },
        "resource":   { "properties": { "type": { "type": "keyword" }, "id": { "type": "keyword" } } },
        "outcome":    { "properties": { "result": { "type": "keyword" }, "errorCode": { "type": "keyword" } } },
        "correlation": { "properties": { "sessionId": { "type": "keyword" }, "transactionId": { "type": "keyword" }, "traceId": { "type": "keyword" } } },
        "service":    { "type": "keyword" },
        "instance":   { "type": "keyword" },
        "chainId":    { "type": "keyword" },
        "seq":        { "type": "long" },
        "prevHash":   { "type": "keyword", "index": false },
        "hash":       { "type": "keyword" },
        "keyId":      { "type": "keyword" }
      }
    }
  }
}' > /dev/null

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
create_data_view audit "Audit events" "audit-*"

# ---- Saved searches on Audit events: the questions auditors ask -------------------------------
# Open them in Discover (Open > the name), set the time range, and narrow down with e.g. resource.id : CUST-1001
# (customer activity has that sample id in its query: replace it).
saved_search() {
  id="$1"; title="$2"; query="$3"; columns="$4"
  echo "Creating saved search '${title}'"
  # searchSourceJSON is a JSON document inside a JSON string, hence the escaped quotes
  source="{\\\"query\\\": {\\\"query\\\": \\\"${query}\\\", \\\"language\\\": \\\"kuery\\\"}, \\\"filter\\\": [], \\\"indexRefName\\\": \\\"kibanaSavedObjectMeta.searchSourceJSON.index\\\"}"
  curl -fsS -X POST "${KIBANA_URL}/api/saved_objects/search/${id}?overwrite=true" \
    -H "kbn-xsrf: true" -H "Content-Type: application/json" \
    -d "{\"attributes\": {\"title\": \"${title}\", \"columns\": ${columns}, \"sort\": [[\"@timestamp\", \"desc\"]],
           \"kibanaSavedObjectMeta\": {\"searchSourceJSON\": \"${source}\"}},
         \"references\": [{\"name\": \"kibanaSavedObjectMeta.searchSourceJSON.index\", \"type\": \"index-pattern\", \"id\": \"audit\"}]}" \
    > /dev/null
}

saved_search audit-customer-activity "Audit: customer activity (all actions of one customer)" \
  "actor.id : CUST-1001 or resource.id : CUST-1001" \
  '["action", "outcome.result", "channel", "source.ip", "source.deviceId", "resource.type", "resource.id", "correlation.transactionId"]'
saved_search audit-record-views "Audit: who viewed a customer record" \
  "action : (CUSTOMER_RECORD_VIEWED or DATA_EXPORTED)" \
  '["resource.id", "actor.id", "actor.role", "action", "channel", "source.ip", "outcome.result"]'
saved_search audit-privileged "Audit: privileged and admin actions" \
  "category : EMPLOYEE and action : (ROLE_CHANGED or CONFIG_CHANGED or DATA_EXPORTED)" \
  '["actor.id", "actor.role", "action", "resource.id", "details.approvedBy", "outcome.result", "outcome.errorCode"]'
saved_search audit-role-changes "Audit: role changes and their approvals (four eyes)" \
  "action : ROLE_CHANGED" \
  '["actor.id", "resource.id", "details.newRole", "details.approvedBy", "outcome.result", "outcome.errorCode"]'
saved_search audit-integrity "Audit: integrity of the audit system (chains, gaps, DLP)" \
  "category : AUDIT_SYSTEM and not action : AUDIT_HEARTBEAT" \
  '["action", "service", "outcome.result", "outcome.errorCode", "resource.id", "details.problem"]'
saved_search audit-failures "Audit: failed and denied actions" \
  "outcome.result : FAILURE" \
  '["category", "action", "actor.id", "outcome.errorCode", "channel", "source.ip"]'
saved_search audit-all "Audit: all records" \
  "not action : (AUDIT_HEARTBEAT or AUDIT_VERIFICATION_COMPLETED)" \
  '["category", "action", "actor.id", "actor.role", "resource.type", "resource.id", "channel", "outcome.result", "service", "seq"]'

# ---- Dashboards (scripts/kibana/*.ndjson: Lens panels, saved searches, the dashboard) ----------
#   microservices-dashboard.ndjson  "Microservices": logs of every microservice (app-logs-*)
#   audit-dashboard.ndjson          "Audit log": the audit records' own view, apart from the operational
#                                   logs (also uses the audit saved searches above)
for file in /scripts/kibana/*.ndjson; do
  echo "Importing $(basename "$file")"
  curl -fsS -X POST "${KIBANA_URL}/api/saved_objects/_import?overwrite=true" \
    -H "kbn-xsrf: true" -F "file=@${file}" \
    | grep -q '"success":true' || { echo "Import of ${file} failed"; exit 1; }
done

# Open Discover on the microservices' logs by default
curl -fsS -X POST "${KIBANA_URL}/api/data_views/default" \
  -H "kbn-xsrf: true" -H "Content-Type: application/json" \
  -d '{"data_view_id": "app-logs", "force": true}' > /dev/null

echo "Kibana is ready: http://localhost:5601/app/discover"
