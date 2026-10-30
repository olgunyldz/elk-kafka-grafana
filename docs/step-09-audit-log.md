# Audit log: regulation records nobody can quietly change

*Step 09 of the ELK + Kafka + Grafana project · branch `step-09-audit-log`*

**Goal:** a separate audit trail for the actions regulators ask about (customer logins and transfers, employees opening customer records, role and parameter changes, the log system's own events), written so that a missing, changed or inserted record is detected, and with nothing sensitive in it.

The logs of steps 04–06 are for **operating** the system: they help find a bug, can be sampled, dropped, rotated, and their level can be turned down. An **audit log** is evidence. BDDK's information systems rules, PCI DSS requirement 10, MASAK and KVKK expect it to be complete, tamper evident, kept for years and reviewed. So it is a different pipeline with different rules:

| | App logs (steps 04–06) | Audit log (this step) |
|---|---|---|
| Purpose | Troubleshooting | Evidence: who did what, when, from where, to what, with which result |
| Written by | Any log statement | Only `AuditLogger`, for defined business actions |
| If it can't be written | The app carries on | **The action is refused** (fail closed) |
| Kafka topic | `app-logs`, 7 days, `min.insync.replicas=1` | `audit-events`, kept forever, `min.insync.replicas=2` |
| Sensitive data | Masked on the way (3 layers) | Never in it: a record with a card number or national ID is **rejected**, not masked |
| Integrity | None | Hash chain with HMAC per app instance, checked every minute |
| Stored in | `app-logs-*`, deleted after days | Archive file (record of truth) + `audit-YYYY.MM` (search), 10 years |
| Reviewed by | Developers, when something breaks | `audit-verifier` every minute, alarms in Grafana, auditors in Kibana |

---

## 1. The big picture

```text
 producer  /api/banking/actions ──┐                                    ┌─> /audit-archive/audit-YYYY-MM-DD.jsonl
 message-api /api/customers, ─────┤   AuditLogger                      │   (append-only, the record of truth)
   /api/admin/*                   ├─> seal: seq, prevHash, HMAC ─> Kafka audit-events ─> Logstash "audit" ─┤
 consumer (decryption of PCI data)┤   send, wait for acks=all          (RF 2, min ISR 2)                   └─> Elasticsearch audit-YYYY.MM
 audit-verifier (its findings) ───┘   failed? → refuse the action                                               (search, Kibana)
                                                                                     │
                       audit-verifier, every minute: reads the archive, checks every chain against
                       its anchors, scans Elasticsearch for unmasked card numbers / national IDs (DLP)
                       ──> log line, audit record (AUDIT_CHAIN_BROKEN, DLP_FINDING, …), Prometheus ──> Grafana
```

New pieces:

| Piece | Where | Role |
|-------|-------|------|
| `io.github.olgunyldz.logmasking.audit` | library | `AuditEvent`, `AuditChain` (sealing), `AuditLogger` (fail-closed writer), `AuditLifecycle` (start, heartbeat, end), `AuditVerifier`, auto-configuration (`audit.enabled`) |
| `BankingController` | producer | `POST /api/banking/actions`: simulated customer-channel actions |
| `EmployeeController` | message-api | Employee actions: view a customer, export, role and parameter changes (four eyes) |
| `audit-verifier` | new app, port 8085 | Chain verification and DLP scan every minute |
| `audit-events` | Kafka | The audit topic |
| `audit` pipeline | Logstash | Topic → archive file + `audit-YYYY.MM` |
| `audit-retention`, template `audit` | Elasticsearch | 3 months hot, read-only after, deleted after 10 years |
| *Audit events* + 7 saved searches | Kibana | The auditors' questions |
| Dashboard *Audit log* | Kibana | The audit records' own view: counts, activity, failures, employee access, integrity |
| Dashboard *Microservices* | Kibana | The microservices' logs (`app-logs-*`): volume, levels, errors and warnings, containers, loggers |
| *Audit log* row | Grafana, *Stack Health* | Chains OK / broken / stale, failed writes, DLP findings, last verification |
| kafka-2, kafka-controller | Kafka | A second broker and a third controller: the audit topic needs two copies of every record |

---

## 2. What gets audited

The simulated actions follow the requirements list (PCI DSS 10.2.1, BDDK, MASAK). Each one writes exactly one record; some write a second one.

| Requirement | Action(s) | Written by |
|-------------|-----------|------------|
| Customer login, logout, failed login, account lock | `LOGIN_SUCCESS`, `LOGIN_FAILED`, `LOGOUT`, `ACCOUNT_LOCKED` (after 3 failures, a second record) | producer |
| OTP sent and verified (never the OTP itself) | `OTP_SENT`, `OTP_VERIFIED` | producer |
| Device or SIM change, password reset, phone / email change | `DEVICE_CHANGED`, `SIM_CHANGE_CHECKED`, `PASSWORD_RESET`, `PHONE_CHANGED`, `EMAIL_CHANGED` | producer |
| Money movements (EFT, FAST, havale, SWIFT), bill payment | `TRANSFER` (`details.transferType`), `BILL_PAYMENT`, with a `TX-…` transaction id | producer |
| Card limit change, loan and card applications | `CARD_LIMIT_CHANGED`, `LOAN_APPLICATION`, `CARD_APPLICATION` | producer |
| Contract approval, consent given / withdrawn (KVKK, open banking) | `CONTRACT_ACCEPTED`, `CONSENT_GIVEN`, `CONSENT_WITHDRAWN` | producer |
| An employee views customer data | `CUSTOMER_RECORD_VIEWED` | message-api |
| Bulk query / export | `DATA_EXPORTED` (`details.rows`, `format`) | message-api |
| Role and permission changes, with approval | `ROLE_CHANGED` (`details.approvedBy`; without a second person: `FAILURE / FOUR_EYES_REQUIRED`, HTTP 403) | message-api |
| Configuration / parameter changes | `CONFIG_CHANGED` (`oldValue`, `newValue`, approval) | message-api |
| Access without identification | The same action with `FAILURE / NO_EMPLOYEE_ID`, HTTP 401 | message-api |
| A system reads card data in clear (PCI 10.2.1.1) | `SENSITIVE_DATA_DECRYPTED`: every record the consumer groups decrypt, with group and `topic/partition/offset` | consumer |
| The log system itself: start, stop, gaps, tampering, review | `AUDIT_CHAIN_START`, `AUDIT_HEARTBEAT`, `AUDIT_CHAIN_END`, `AUDIT_CHAIN_BROKEN`, `AUDIT_LOG_INTERRUPTED`, `AUDIT_ARCHIVE_UNREADABLE`, `AUDIT_VERIFICATION_COMPLETED`, `DLP_FINDING` | every app, audit-verifier |

```bash
curl -s localhost:8081/api/banking/actions        # the customer actions and their resource types
curl -s -X POST localhost:8081/api/banking/actions -H "Content-Type: application/json" \
  -d '{"action":"TRANSFER","customerId":"CUST-1001","channel":"mobile","deviceId":"DEV-1","sessionId":"SES-1",
       "details":{"transferType":"FAST","amount":"1500","currency":"TRY"}}'
# {"action":"TRANSFER","outcome":"SUCCESS","auditEventId":"23b3…","chainId":"f536…","seq":3,"alsoAudited":[]}

curl -s localhost:8084/api/customers/CUST-1001 -H "X-Employee-Id: EMP-042" -H "X-Employee-Role: teller"
curl -s -X POST localhost:8084/api/admin/roles -H "Content-Type: application/json" -H "X-Employee-Id: EMP-042" \
  -d '{"userId":"EMP-150","newRole":"ops-admin","approvedBy":"EMP-007"}'
```

Or use the **Audit log** section of the producer page (http://localhost:8081): customer actions on the left, employee actions on the right, every written record (or refusal) in the table below.

---

## 3. The record

One JSON object per action, keys sorted, as it lands in the archive:

```json
{"action":"TRANSFER","actor":{"id":"CUST-1001","type":"customer"},"category":"CUSTOMER_CHANNEL",
 "chainId":"f536b548-d7e8-4082-8c0f-fb0e88f2abaa","channel":"mobile",
 "correlation":{"sessionId":"SES-1","traceId":"ef7397ac3d0a5cb3967f199f3cc11e31","transactionId":"TX-ae496445"},
 "details":{"amount":"1500","currency":"TRY","transferType":"FAST"},
 "eventId":"23b36f28-cccc-475d-b0c6-d8d57b7a39a0",
 "hash":"69314810911ca8fe480a00af672ef4ff3072229c0a1a77052160acbebadc6adb",
 "instance":"58e25f955fec","keyId":"audit-v1","outcome":{"result":"SUCCESS"},
 "prevHash":"f8142fc49416bc2bb22f1ad0c0fd27c73c499fe0e4dbbf2e5dc54f3dd45eb4a1",
 "resource":{"id":"CUST-1001","type":"account"},"seq":3,"service":"producer",
 "source":{"ip":"172.19.0.1"},"timestamp":"2026-10-30T10:20:03.823Z"}
```

| Question | Field(s) | Notes |
|----------|----------|-------|
| Who | `actor.type` (`customer`, `employee`, `system`), `actor.id`, `actor.role` | A customer number or employee id, **never the national ID (TCKN)** |
| What | `category`, `action`, `details` | `details` holds only non-sensitive facts |
| When | `timestamp` | UTC, millisecond precision, from the app's clock (NTP: see Limits) |
| From where | `channel`, `source.ip`, `source.deviceId`, `source.appVersion` | IP from `X-Forwarded-For` or the connection |
| On what | `resource.type`, `resource.id` | Account, card, customer, user, parameter, Kafka record… |
| Result | `outcome.result` (`SUCCESS` / `FAILURE`), `outcome.errorCode` | Failures are audited too |
| Correlation | `correlation.sessionId`, `transactionId`, `traceId` | `traceId` comes from the current span (step 08): the audit record links to the request's trace and logs |
| Integrity | `service`, `instance`, `chainId`, `seq`, `prevHash`, `keyId`, `hash` | See next section |

**Sensitive values are refused, not masked.** Masking would put a half-hidden value into evidence that must stay unchanged for 10 years. Before sealing, `AuditLogger` runs the step 03 detector over every caller-supplied field (actor, resource, source, correlation, details). A card number, IBAN, national ID, email or phone number throws `IllegalArgumentException` (HTTP 400), and nothing is written:

```text
POST /api/banking/actions  {"action":"TRANSFER", …, "details":{"note":"card 4111 1111 1111 1111"}}
400  Audit record rejected: details.note contains card number. Use an internal id or token instead.
```

---

## 4. Tamper evidence: one hash chain per app instance

When an app starts, `AuditChain` picks a random `chainId`. Every record gets the next `seq`, the previous record's hash, and its own hash:

```text
hash(n) = HMAC-SHA256(key, hash(n-1) + "\n" + canonicalJson(record n without "hash"))
hash(0) = 64 zeros
canonicalJson: keys sorted at every level, null fields left out — the same bytes for the writer and the verifier
```

The first record of a chain is `AUDIT_CHAIN_START`, a heartbeat follows every 60 s (`AUDIT_HEARTBEAT`, so a quiet app still proves it is alive), and a clean shutdown writes `AUDIT_CHAIN_END`. Each instance has its own chain (the two consumer replicas have two), so nothing has to be shared or locked across processes, and Kafka's partition order doesn't matter: the verifier sorts by `seq`.

Why HMAC and not a plain SHA-256: with a plain hash, someone who can edit the archive can also recompute every later hash. With HMAC they need the key, which the archive's writer (Logstash) and readers never get. The key is `AUDIT_HMAC_KEY` (base64, at least 32 bytes); `keyId` names it, so keys can be rotated and old records still verify.

What the verifier catches, each one tried in `AuditVerifierTest` and most of them on the running stack (section 10):

| Attack | Detected as |
|--------|-------------|
| Change a field (`amount` 1500 → 15) | `hash does not match the record (modified or forged)` at that `seq` |
| Delete a record | `gap: seq N to N missing (deleted records)` |
| Delete the first record | `chain does not start with AUDIT_CHAIN_START` |
| Insert a forged record (without the key) | Duplicate `seq`, or the HMAC fails |
| Re-sign a record with another key | Unknown `keyId`, or the HMAC fails |
| Swap two records | `prevHash` no longer links |
| Cut off the end of a chain | The **anchor** (below) has a higher `seq` than the archive |
| Delete a whole chain or file | The anchor names a chain that isn't in the archive any more |
| Stop an app's audit trail (kill it, block it) | No record, not even a heartbeat, for 3 minutes and no `AUDIT_CHAIN_END`: **STALE** |

**Anchors.** A chain alone can't show that its last records were cut off. After each run the verifier stores the last verified `seq` and `hash` of every chain in `/audit-anchors/anchors.json`, a separate volume only the verifier can write. Next run, the archive must still contain that exact record. In production the anchors go somewhere the archive's admins can't reach (another account, a timestamping service, or a WORM bucket).

---

## 5. Fail closed, and why Kafka needs two brokers now

PCI DSS and BDDK want every audited action logged. The only way to be sure is to **not carry out an action whose record wasn't written**. `AuditLogger.log()` seals the record, sends it and waits for the broker's acknowledgement (`acks=all`, idempotent, 15 s at most). If that fails it throws `AuditWriteException`, the controllers answer **503**, and the chain doesn't advance: the next record reuses the `seq`, so a refused action leaves no gap.

"Written" must mean more than "one broker has it". `audit-events` has replication factor 2 and **`min.insync.replicas=2`**: a write is acknowledged only when both brokers hold it. With one broker down, audited actions stop (on purpose) while the ordinary topics (`min.insync.replicas=1`) keep working. That's why this step brings Kafka back from one node (step 05) to:

| Node | Roles | Host port |
|------|-------|-----------|
| `kafka-1` | broker, controller | `19092` |
| `kafka-2` | broker, controller | `29092` |
| `kafka-controller` | controller only (no data, 256 MB heap) | – |

Three controllers keep a KRaft majority (2 of 3) when any one node stops; with two brokers, every topic gets replication factor 2. `scripts/create-topics.sh` raises the replication factor of existing topics (and of `__consumer_offsets`) with `kafka-reassign-partitions.sh`, so it works on volumes from earlier steps too, and applies per-topic configs:

```text
audit-events:3:2:retention.ms=-1,cleanup.policy=delete,min.insync.replicas=2
```

`retention.ms=-1`: Kafka keeps audit records until they're deleted on purpose; the archive is the long-term copy, but replaying the topic rebuilds both the archive and the index.

---

## 6. The archive and the search copy

A third Logstash pipeline, `audit` (consumer group `logstash-audit`, `auto_offset_reset => earliest`), writes every record twice:

1. **The archive**: the record exactly as it came from Kafka (`codec => plain`, the raw line kept in `@metadata` before parsing), appended to `/audit-archive/audit-YYYY-MM-DD.jsonl`, the day being the record's own `timestamp`. Files are `0640`, owned by Logstash; the verifier reads them through Logstash's group (`group_add: ["1000"]`) and mounts the volume read-only. **This is the record of truth.**
2. **Elasticsearch `audit-YYYY.MM`**: parsed, `@timestamp` from the record's `timestamp`, `eventId` as document id with `action => create` (a re-delivered record isn't indexed twice, an indexed record is never overwritten). For searching only: an index can be changed, so the proof is always the archive's chain.

There is **no masking filter** in this pipeline, on purpose: it would change the bytes and break the HMAC, and there is nothing to mask (section 3).

Retention, set by `scripts/kibana-setup.sh` (policy `audit-retention`, index template `audit`):

| Rule | Requirement | Here |
|------|-------------|------|
| At least 12 months, last 3 months immediately available | PCI DSS 10.5.1 | Hot for 90 days, then read-only and force-merged, still searchable |
| 8 years | MASAK (transaction records) | Covered by 10 years |
| 10 years | Turkish Commercial Code (TTK) | Deleted after 3650 days |
| Destroy when no longer needed | KVKK | ILM deletes the index; archive files need the same rule on their storage (object lifecycle) |
| Records stay in Turkey | BDDK (data localisation) | Everything runs locally here; in production, storage and backups in Turkish data centres |

The template also maps the audit fields properly (`keyword` ids, `ip` for `source.ip`, `details.*` as keywords).

---

## 7. audit-verifier

A small Spring Boot app (port 8085, no API of its own). Every minute (`verifier.interval`, first run 30 s after start):

1. Reads every `*.jsonl` in the archive. If it can't (permissions, missing volume), that counts as broken and is audited as `AUDIT_ARCHIVE_UNREADABLE`: an unreadable archive must never look like "0 chains, all fine".
2. Runs `AuditVerifier` from the library with the anchors and every known key (`verifier.keys`, by key id).
3. Logs one line per chain, e.g. `Audit chain OK: f536… (producer/58e25f955fec) seq 1..17`, `Audit chain BROKEN: … at seq 3: hash does not match the record (modified or forged)`.
4. Writes `AUDIT_CHAIN_BROKEN` / `AUDIT_LOG_INTERRUPTED` once per chain, and `AUDIT_VERIFICATION_COMPLETED` every run (with counts): PCI DSS 10.4.1's daily review, done every minute, and itself on the record.
5. Saves the new anchors.
6. **DLP scan**: searches the documents indexed since the last run in `app-logs-*`, `infra-logs-*`, `elk-logs-*`, `kafka-messages-*` and `audit-*` for card numbers (Luhn and a card network prefix) and national IDs (check digits), skipping step 03's encrypted tokens. A hit is logged as `DLP: unmasked card number in <index> document <id>` (where, never the value) and audited as `DLP_FINDING`. Up to 5000 documents per run; when a page is full, the next run continues from the last document instead of skipping ahead.

Metrics on `/actuator/prometheus`:

| Metric | Meaning |
|--------|---------|
| `audit_verifier_chains{status="ok\|ended\|stale\|broken"}` | Chains by result of the last run |
| `audit_verifier_records_verified` | Records checked in the last run |
| `audit_verifier_last_run_seconds` | Unix time of the last run |
| `audit_dlp_findings`, `audit_dlp_documents_scanned` | Last DLP scan |
| `audit_records_total{action, result="written\|failed"}` | Every app: audit records written / refused |

---

## 8. The audit view in Kibana

Audit records have their own view, apart from the operational logs (*App logs*, *Infra logs*, *All logs*, *Kafka messages*): the data view **Audit events** (`audit-*`), the dashboard **Audit log**, and seven saved searches. All are created by `scripts/kibana-setup.sh`; the dashboard comes from `scripts/kibana/audit-dashboard.ndjson`.

**Dashboard *Audit log*** (http://localhost:5601/app/dashboards#/view/audit-dashboard, last 24 h, refreshes every minute):

| Panel | Shows |
|-------|-------|
| Audit records · Failed or denied actions · Integrity alerts · Customer records viewed by employees | Counts for the time range; *Integrity alerts* = `AUDIT_CHAIN_BROKEN`, `AUDIT_LOG_INTERRUPTED`, `AUDIT_ARCHIVE_UNREADABLE`, `DLP_FINDING` |
| Audit records over time, by category | `CUSTOMER_CHANNEL`, `EMPLOYEE`, `SYSTEM`, `AUDIT_SYSTEM` per interval |
| Top business actions | Customer and employee actions, without the audit system's own records |
| Failures by action and error code | `WRONG_PASSWORD`, `FOUR_EYES_REQUIRED`, `NO_EMPLOYEE_ID`, `STALE`, … |
| Employee activity | Per employee and role: which actions, how often |
| Integrity · Who viewed a customer record · All records | The saved searches below, as tables |

Type a filter in the dashboard's query bar to answer a specific question across every panel, e.g. `resource.id : CUST-1001 or actor.id : CUST-1001`, or `actor.id : EMP-077`.

**Saved searches** (*Discover → Open*):

| Auditor asks | Saved search | Query |
|--------------|--------------|-------|
| Everything customer X did between two dates | *Audit: customer activity* | `actor.id : CUST-1001 or resource.id : CUST-1001` (replace the sample id) |
| Who looked at customer records | *Audit: who viewed a customer record* | `action : (CUSTOMER_RECORD_VIEWED or DATA_EXPORTED)`; add `and resource.id : <customer>` for one customer |
| Privileged / admin actions | *Audit: privileged and admin actions* | `category : EMPLOYEE and action : (ROLE_CHANGED or CONFIG_CHANGED or DATA_EXPORTED)` |
| Role changes and who approved them | *Audit: role changes and their approvals* | `action : ROLE_CHANGED` (column `details.approvedBy`) |
| Prove the logs weren't changed, and show any gap | *Audit: integrity of the audit system* | `category : AUDIT_SYSTEM and not action : AUDIT_HEARTBEAT` (chain starts and ends, verification results, broken chains, interruptions, DLP findings) |
| Failed and denied actions | *Audit: failed and denied actions* | `outcome.result : FAILURE` |
| The full trail | *Audit: all records* | Everything except heartbeats and verification runs |
| Scan the logs for clear card numbers / national IDs | – | The DLP scan, every minute; findings under `action : DLP_FINDING` |

For a formal answer, the evidence is the archive plus the verifier's report, not the index: run the verifier over the archive files for the period and attach its output.

### And a dashboard for the microservices' own logs

The operational side gets a dashboard too: **Microservices** (http://localhost:5601/app/dashboards#/view/microservices-dashboard, last hour, refreshes every 30 s), on *App logs* (`app-logs-*`): producer, both consumer replicas, message-api and audit-verifier.

| Panel | Shows |
|-------|-------|
| Log lines · Errors · Warnings · Services logging | Counts for the time range |
| Log lines over time, by service | Volume per minute |
| Log levels per service | INFO / WARN / ERROR split per service |
| Errors and warnings over time, by service | Spikes, e.g. a broker going down |
| Where errors and warnings come from | Per service and logger (grouped by logger, not message: messages carry ids, so each text would count once) |
| Containers | Per container: the consumer replicas show up separately, with their own error counts |
| Busiest loggers | Who writes the most lines |
| *App logs: errors and warnings*, *App logs: all microservices* | Saved searches as tables, with `trace.id` to jump to the trace (step 08) |

Filter in the query bar, e.g. `service.name : consumer` or `trace.id : "<id>"`. Both dashboards are `scripts/kibana/*.ndjson`, imported by `kibana-setup` on every start. In `app-logs-*` the text fields are aggregated through their `.keyword` sub-fields (`service.name.keyword`, `log.level.keyword`, …).

---

## 9. Grafana

*Stack Health* has a new row, **Audit log (step 09)**:

| Panel | Red when |
|-------|----------|
| Chains OK | – |
| Chains broken | ≥ 1 |
| Chains stale | ≥ 1 (orange) |
| Audit writes failed (1h) | ≥ 1: actions were refused |
| DLP findings | ≥ 1 |
| Last verification | Over 5 minutes ago |
| Audit records written per minute, by action | – |
| Chains by status | Over time |

audit-verifier is also a scrape target and a health check like every other app (28 targets in total).

---

## 10. Things to try

All tampering is done as the archive's writer, inside the Logstash container (the verifier itself can't write the archive):

```bash
F=/audit-archive/audit-$(date -u +%F).jsonl
docker exec logstash sh -c "cp $F /tmp/archive.bak"                       # keep a copy

# Change a value → within a minute: "Audit chain BROKEN: … at seq N: hash does not match the record"
docker exec logstash sed -i 's/"amount":"1500"/"amount":"15"/' $F
docker logs -f audit-verifier 2>&1 | grep -o '"message":"Audit[^"]*'

# Undo it → the chain is OK again. Delete a record instead → "gap: seq N to N missing"
docker exec logstash sed -i 's/"amount":"15"/"amount":"1500"/' $F
docker exec logstash sed -i '/FOUR_EYES_REQUIRED/d' $F
docker exec logstash sh -c "grep FOUR_EYES_REQUIRED /tmp/archive.bak >> $F"   # put it back (order doesn't matter)
```

| Do | See |
|----|-----|
| Producer page → *Audit log* → `login` three times with *Make the action fail* | `LOGIN_FAILED` ×3, then `ACCOUNT_LOCKED` as a second record |
| *Transfer with a card number in the details* | HTTP 400, nothing written |
| `customerId` = a valid national ID (e.g. `10000000146`) | HTTP 400: who is a token, never the TCKN |
| Employee: *Change a user's role* with *Approved by* = the employee's own id | `ROLE_CHANGED FAILURE / FOUR_EYES_REQUIRED`, 403, audited |
| Employee id empty → *View customer record* | `FAILURE / NO_EMPLOYEE_ID`, 401, audited as `anonymous` |
| `docker compose stop kafka-2`, then any audited action | 503 after ~10 s: *Audit record could not be written; the action is not allowed*. `POST /api/events` still works. Start it again: the next action succeeds, the chain has no gap |
| `docker compose kill message-api` (no clean stop), wait 3+ min | Its chain turns **STALE**, `AUDIT_LOG_INTERRUPTED` is written, *Chains stale* goes orange |
| Index a document with a clear card number: `docker exec elasticsearch curl -s -XPOST "localhost:9200/app-logs-dlp-test/_doc?refresh=true" -H "Content-Type: application/json" -d '{"@timestamp":"<now, UTC>","message":"card 4111111111111111"}'` | `DLP: unmasked card number in app-logs-dlp-test document …`, `DLP_FINDING`, *DLP findings* = 1. Delete the index afterwards |
| Kibana → dashboard *Audit log* | Counts, activity over time, failures, employee activity; *Integrity alerts* goes up with each chain you broke |
| Kibana → *Audit: integrity of the audit system* | Chain starts and ends, every verification run, and the broken chains you caused |

---

## 11. Corrections to earlier articles

The earlier articles describe their own step and are left unchanged. On this branch, these statements are out of date:

| Earlier article says | Now |
|----------------------|-----|
| Step 05: Kafka is down to **one broker** `kafka-1`, host port `19092`, replication factor 1, `min.insync.replicas` 1, no failover | Two brokers (`kafka-1`, `kafka-2`) and three KRaft controllers (both brokers plus `kafka-controller`). Host ports `19092` and `29092`. Replication factor 2 for every topic and `__consumer_offsets`; `min.insync.replicas` 1, except `audit-events` (2). Any one node can stop: tested for each of the three, messages keep flowing. |
| Step 05–08: bootstrap `kafka-1:9092` (apps, Logstash, Filebeat, kafka-ui, kafka-exporter) | `kafka-1:9092,kafka-2:9092` everywhere; apps from the host: `localhost:19092,localhost:29092`. |
| Step 01/05: `create-topics.sh` lists `name:partitions:replication` (`app-logs:3:1`) | `name:partitions:replication:configs` (`app-logs:3:2:retention.ms=604800000`); it also raises the replication factor of existing topics and of `__consumer_offsets`, and applies the configs with `kafka-configs.sh`. |
| Step 02: the producer's timeouts are Kafka's defaults | `request.timeout.ms` 10 s, `delivery.timeout.ms` 30 s, `max.block.ms` 10 s: with a broker gone, a send fails in seconds instead of hanging for 2 minutes. |
| Step 05: Kibana data views *App logs*, *Infra logs*, *All logs*, *Kafka messages* | Plus *Audit events* (`audit-*`), the dashboards *Audit log* and *Microservices*, and nine saved searches (seven on audit, two on app logs); `kibana-setup.sh` also creates the ILM policy `audit-retention` and the index template `audit`. Logstash now waits for `kibana-setup`. |
| Step 05: two Logstash pipelines | Three: plus `audit` (`audit-events` → archive file + `audit-YYYY.MM`). Logstash mounts the `audit-archive` volume. |
| Step 05: Filebeat's infra logs come from `kafka-1`, `kafka-init`, `kafka-ui` | Plus `kafka-2` and `kafka-controller`. `filebeat-apps` also reads `audit-verifier`; `filebeat` skips it. |
| Step 03: the library's beans | Plus the audit beans (`AuditAutoConfiguration`, on when `audit.enabled=true`): its own `KafkaTemplate<String,String>`, `AuditLogger`, `AuditLifecycle`, the `audit.records` counter. `PciDecryptingRecordInterceptor` audits every decryption (`SENSITIVE_DATA_DECRYPTED`); if that record can't be written, the record stays encrypted. 176 library tests. |
| Step 08: message-api has no Kafka access and only answers `/api/messages` | It has `spring-kafka` and the PCI and audit keys, writes audit records, and serves the employee endpoints (`/api/customers/*`, `/api/admin/*`, CORS for the producer page). It waits for `kafka-init`. |
| Step 07: Prometheus scrapes the broker at `kafka-1:7071`; TCP checks for `kafka-1:9092` | `kafka-1:7071`, `kafka-2:7071`, `kafka-controller:7071`; TCP checks for `kafka-2:9092` and `kafka-controller:9093` too; `audit-verifier:8085` scraped and health-checked. 28 targets. |
| Step 07: *Kafka* dashboard, *Brokers up* | Counts the controller too (shows 3), since all three nodes run the JMX agent. |
| Step 07: *Stack Health* has four rows | Five: plus *Audit log (step 09)*. |
| Step 05–08: "the stack needs about 5–6 GB" | About 8.7 GB with everything running (the second broker, the controller, audit-verifier, and Elasticsearch's page cache). Give Docker 10 GB or more. |

**Coming from step 08**: the step 05 broker's metadata only knows one controller, so the Kafka volumes can't be reused. Remove them once: `docker compose down` then `docker volume rm elk-kafka-grafana_kafka-1-data` (or `docker compose down -v` for a clean start).

---

## 12. What we checked

| Check | Result |
|-------|--------|
| Customer actions | `LOGIN`, `TRANSFER`, 3 × failed login → 200, `LOGIN_FAILED` ×3 and `ACCOUNT_LOCKED` |
| Sensitive values | Card number in `details` → 400 *Audit record rejected: details.note contains card number*; a valid national ID as `customerId` → 400; nothing written |
| Employee actions | View with id → 200 (masked profile); without id → 401 `NO_EMPLOYEE_ID`; role change approved by oneself → 403 `FOUR_EYES_REQUIRED`; approved by another → 200. All four audited |
| Pipeline | 20 records on `audit-events` (3 partitions, lag 0 for `logstash-audit`); 20 documents in `audit-2026.10` with the `audit-retention` policy; the archive file has the same lines |
| Verification | 6 chains: producer, message-api, 2 consumer replicas, audit-verifier OK; an earlier verifier run ENDED (clean stop) |
| Tampering | `amount` 1500 → 15: producer chain BROKEN at seq 3 *hash does not match*; undone → OK. One record deleted: message-api chain BROKEN at seq 4 *gap: seq 4 to 4 missing*; put back → OK. Both written as `AUDIT_CHAIN_BROKEN` and visible in `audit-*` |
| Fail closed | `kafka-2` stopped: transfer and employee view → 503 after 10 s; `POST /api/events` → 202. `kafka-2` back: transfer → 200 at the next `seq`; all chains OK, no gap |
| DLP | A planted `4111111111111111` in `app-logs-dlp-test` → found in the next run, `audit_dlp_findings` = 1, the log line names the document, not the number. 0 findings in the real indices |
| Kibana dashboard *Microservices* | Imported by `kibana-setup` (13 objects); rendered with every panel filled: 4 services, consumer replicas per container, errors and warnings by logger |
| Kibana dashboard *Audit log* | Imported by `kibana-setup` (9 objects); rendered in a headless browser with every panel filled: 202 records, 24 failures, employee activity for 3 employees, top business actions, the three tables |
| Permissions | The verifier can read the archive, can't write it (read-only mount); only it can write the anchors |
| Tests | 176 library tests (14 `AuditVerifierTest`, 5 `AuditLoggerTest`, 2 for the auditing interceptor) + 4 `DlpScannerTest` |
| Prometheus | 28 targets up, 0 down; `audit_records_total`, `audit_verifier_*`, `audit_dlp_*` present |
| Memory | 8.7 GB for the whole stack (Docker VM 11.7 GB); audit-verifier about 315 MB, kafka-controller about 310 MB |

---

## 13. Limits, and what production needs

- **Keys in environment variables.** `AUDIT_HMAC_KEY` (like `DATA_ENCRYPTION_KEY`) defaults to a **public lab key** from the repo. In production it comes from a KMS or HSM, the apps get sign-only access, and the people who run the archive never see it. Rotate by adding `audit-v2` to the verifier's `verifier.keys` first, then switching the apps' `audit.key-id`.
- **The archive is a Docker volume.** Anyone with root on the Docker host can rewrite it; the chain will show it, but can't prevent it. Production: WORM storage (S3 Object Lock in compliance mode, an immutable Azure blob, a write-once appliance) with the retention set on the storage, plus a copy in a separate account. Same for the anchors.
- **Rewriting the whole chain** is possible for someone who has both the archive and the HMAC key. That's what the key separation and the external anchors are for.
- **Time.** Records use the app's clock. PCI DSS 10.6 and BDDK want all systems synchronised to a trusted time source (NTP from a national/approved source); in Docker the containers use the host's clock. The verifier doesn't check clock skew.
- **SIEM.** The lab's "SIEM" is Kibana plus the verifier. A real bank forwards `audit-events` (or the archive) to its SIEM (QRadar, Splunk, Sentinel…) for correlation and alerting, and to a SOC that reviews it.
- **Privileged access (PAM).** Admin sessions on servers and databases (sudo, DBA access) are audited by a PAM tool, not by the apps. Here only application-level employee actions are covered.
- **STALE is final.** A killed instance never writes `AUDIT_CHAIN_END`, so its chain stays STALE until someone investigates. A real setup adds an "acknowledged" step (a signed record that closes the chain with a reason).
- **The verifier re-reads the whole archive every minute.** Fine for a lab; at volume, verify incrementally from the anchors and run full checks daily.
- **One verifier, alerts only on dashboards.** Alert rules (Grafana alerting to mail or chat) for *broken*, *stale*, *write failures* and *DLP findings* are the next step.
- **Simulated identities.** `X-Employee-Id` is a header anyone can set. In a real system the identity comes from the authenticated session (SSO, mTLS), and the audit record takes it from there.
- **DLP covers card numbers and national IDs** in `message` and error fields of the indexed documents. CVV/CVC can't be found reliably by pattern (any 3 digits); that's prevented upstream (step 03 removes it before Kafka, step 06 masks it in logs).

---

## Next step

Everything is recorded, checked and visible on dashboards, but someone still has to look. Next: **alerting**, with Grafana alert rules for broken or stale audit chains, failed audit writes, DLP findings, consumer lag and failing services, sent where someone will act on them.
