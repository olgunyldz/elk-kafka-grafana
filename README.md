# ELK + Kafka + Grafana

A log and metrics pipeline built one step at a time. Each step is a branch.

| Step | Branch | Goal |
|------|--------|------|
| 01 | `step-01-kafka-init` | A running Kafka cluster and topics, used from the CLI |
| 02 | `step-02-producer-consumer` | Spring Boot producer and consumer |
| 03 | `step-03-protect-kafka-data` | No card or personal data readable in Kafka |
| 04 | `step-04-logging` | App logs as masked ECS JSON, shipped to Kafka |
| 05 | `step-05-elk` | Kafka messages and all logs searchable in Kibana |
| 06 | `step-06-mask-logging` | No sensitive data in any log, at three layers |
| 07 | `step-07-monitoring` | Metrics and health of the broker, apps and containers in Grafana |

# Step 07: Monitoring

**Goal:** metrics and health of the broker, the apps and every container, in Grafana.

For the full design, read [docs/step-07-monitoring.md](docs/step-07-monitoring.md).

Open **Grafana at http://localhost:3000** (no login). Prometheus is at http://localhost:9090.

| Source | Metrics |
|--------|---------|
| `kafka-1` with the JMX exporter agent (`:7071`) | Traffic per topic, partitions, controller, request time, heap |
| `kafka-exporter` | Lag of every consumer group, ours and Logstash's |
| `producer`, `consumer` (`/actuator/prometheus`) | Kafka client, `KafkaTemplate`, `@KafkaListener`, HTTP and JVM metrics |
| `blackbox-exporter` | Up/down of every service (HTTP health URL or TCP port) |
| `cadvisor` | CPU, memory and network per container |

| Dashboard | Answers |
|-----------|---------|
| **Kafka** (home) | Is the cluster healthy, and is everyone keeping up? |
| **Kafka Producer** | Throughput, latency, batching, errors, back-pressure |
| **Kafka Consumer** | Lag, poll loop, processing time, rebalances, commits |
| **Stack Health** | Is every service up? |
| **Containers** | Which container uses how much CPU and memory? |

Try it: `docker compose stop consumer`, send a few messages from http://localhost:8081, and watch the lag rise on *Kafka* and the consumer turn red on *Stack Health*. Then `docker compose start consumer` and watch it catch up.

---

# Step 06: Masking in three layers

**Goal:** no sensitive data in any log, at three layers.

For the full design, read [docs/step-06-mask-logging.md](docs/step-06-mask-logging.md).

| Layer | Where | Covers |
|-------|-------|--------|
| 1. App (step 04) | `log-masking`: `PciLogger`, `MaskingAppender` | Logs of apps that use the library |
| 2. Filebeat | `replace` processors in `filebeat/filebeat.yml` and `filebeat/filebeat-apps.yml` | Every container log Filebeat ships |
| 3. Logstash | `logstash/pipeline/shared/mask_sensitive.rb` in both pipelines | Everything stored in Elasticsearch: app logs and Kafka messages |

Card numbers keep their last 4 digits; CVV, PIN, passwords and tokens become `***`; IBANs, national IDs, emails, phones, names and addresses are masked too. Layer 3 checks checksums and keys and leaves the encrypted tokens from step 03 alone. Its self-tests run at every pipeline start, and a failing test stops the pipeline:

```bash
docker compose up -d --force-recreate logstash filebeat filebeat-apps
docker compose logs logstash | grep "Test run complete"   # passed: 6, failed: 0 (once per pipeline)
```

Try layer 2 on its own: write an unmasked line into the broker's console, then search *Infra logs* in Kibana for `LEAKTEST`:

```bash
docker exec kafka-1 sh -c "echo '[2026-10-30 05:30:00,000] INFO LEAKTEST card 4111 1111 1111 1111 password=hunter2 mail jane.doe@example.com' > /proc/1/fd/1"
```

---

# Step 05: ELK

**Goal:** Kafka messages and all logs searchable in Kibana.

For the full design, read [docs/step-05-elk.md](docs/step-05-elk.md).

| What | Path | Kibana data view |
|------|------|------------------|
| Kafka messages | Kafka → Logstash (group `logstash`) → `kafka-messages-<topic>` | *Kafka messages* |
| App logs (microservices) | producer, consumer → `filebeat-apps` → Kafka `app-logs` → Logstash (group `logstash-app-logs`) → `app-logs-YYYY.MM.dd` | *App logs* (default) |
| Infra logs | kafka-1, kafka-init, kafka-ui → `filebeat` → `infra-logs-YYYY.MM.dd` | *Infra logs* |
| ELK logs | elasticsearch, logstash, kibana, kibana-setup → `filebeat` → `elk-logs-YYYY.MM.dd` | — |
| Every log except the microservices' | infra + ELK | *All logs* |

**One broker now.** To fit ELK in memory, Kafka runs as a single node (`kafka-1`, host port `localhost:19092`), with replication factor 1. The whole stack uses about 5 GB, so give Docker at least 6 GB.

```bash
# Coming from step 04: the old three-broker volumes can't be reused
docker compose down -v --remove-orphans
docker volume rm elk-kafka-grafana_kafka-2-data elk-kafka-grafana_kafka-3-data

docker compose up -d --build        # Kafka, apps, Elasticsearch, Logstash, Kibana, both Filebeats
```

Then open **Kibana at http://localhost:5601** → Discover. Send some samples from the producer page (http://localhost:8081) and try:

| Data view | Query |
|-----------|-------|
| App logs | `log.level : ERROR` |
| Infra logs | `container.name : "kafka-1" and message : "ERROR"` |
| App logs | `service.name : consumer and message : "group event-consumer"` |
| Kafka messages | `kafka.pci_protected : true` |
| Kafka messages | `payload.payload.card.number : pan\:v1\:4444*` (card ending 4444, no key needed) |

Elasticsearch has no published port (it's on the internal `data-layer`):

```bash
docker exec elasticsearch curl -s 'localhost:9200/_cat/indices?v&s=index'
```

| Service | Image | Port |
|---------|-------|------|
| elasticsearch | `elasticsearch:9.5.3` | — |
| logstash | `logstash:9.5.3` | — |
| kibana | `kibana:9.5.3` | 5601 |
| filebeat (infrastructure) | `elastic/filebeat:9.5.3` | — |
| kibana-setup (one-shot) | `curlimages/curl:8.11.1` | — |

---

# Step 04: Logging

**Goal:** app logs in a form that can be shipped and searched, with no card or personal data in them.

For the full design, read [docs/step-04-logging.md](docs/step-04-logging.md).

- **Masked:** `log-masking` gains a `logging` package. App code logs through `PciLogger`, and a `MaskingAppender`, installed automatically, masks everything else, Spring and Kafka client logs included. Card numbers keep their last 4 digits; CVV, passwords and tokens become `***`; emails, phones, IBANs, national IDs, names and addresses are masked too.
- **Structured:** in Docker, both apps log ECS JSON (`LOGGING_STRUCTURED_FORMAT_CONSOLE: ecs`), one object per line. From the IDE they log plain text.
- **Shipped:** `filebeat-apps` reads the producer's and consumer's Docker logs and sends each line to the Kafka topic `app-logs`.
- **New topic:** `POST /api/logs` now publishes to `app-log-messages`, so `app-logs` holds only real app logs.

| App | Logs |
|-----|------|
| producer | `Received for <topic>: key=… value=…` (masked), then `Sent to <topic> partition … offset …: key=… value=…` (as stored in Kafka) |
| consumer | `[group <group>] Key: … \| Value: … \| Partition: … \| Offset: …` (masked) |

```bash
docker compose up -d --build                       # also starts filebeat-apps
docker compose logs -f consumer                     # ECS JSON, masked
docker exec kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka-1:9092 \
  --topic app-logs --from-beginning --max-messages 3  # the same lines, shipped by Filebeat
```

Masking settings (`application.yml`, or environment variables like `LOGGING_MASKING_DISABLED_RULES`):

```yaml
logging.masking:
  enabled: true
  disabled-rules: ""          # secrets, personal, expiry, iban, card, tckn, email, phone
  extra-sensitive-keys: ""    # e.g. otp
  extra-personal-keys: ""     # e.g. mother_maiden_name
```

---

# Step 03: Protect card and personal data in Kafka

**Goal:** no card or personal data readable in Kafka.

For the full design, read [docs/step-03-protect-kafka-data.md](docs/step-03-protect-kafka-data.md).

A new library module, [springboot/log-masking](springboot/log-masking), protects every message before it leaves the producer:

| Data | Examples | Sent to Kafka as |
|------|----------|------------------|
| Card security data | CVV, PIN, track data | **removed** |
| Credentials | passwords, API keys, tokens, `Authorization: Bearer …` | **removed** |
| Card data | `4111 1111 1111 1111` (Luhn + card network checked), expiry dates | **encrypted** `pan:v1:1111:…`, `exp:v1::…` (AES-256-GCM) |
| Personal data | name, email, phone, address, birth date, national ID, IBAN | **encrypted** `email:v1:example.com:…`, `pii:v1::…` |

Tokens keep a readable hint (last 4 card digits, email domain, …) so values can be told apart without the key.

- The producer sends through `PciKafkaTemplate`. Responses include `sent` (the value as stored in Kafka), `removedValues` and `encryptedValues`, and a key containing sensitive data is rejected with `400`.
- The consumer's groups (`pci.decrypt.consumer-groups`, `*` for all) get the values decrypted by `PciDecryptingRecordInterceptor`. Every other reader, such as `kafka-ui` or a console consumer, sees only tokens.
- On the web page, use the **sample buttons** to send messages with card and personal data. The **Sent to Kafka** column shows what was removed and encrypted, and the value exactly as Kafka stores it.

### The encryption key

Both apps read `pci.data-encryption-key`. Compose sets it from `DATA_ENCRYPTION_KEY`, which defaults to a **public lab key** committed in this repo. Anyone can decrypt with that key, so use your own for anything real:

```bash
echo "DATA_ENCRYPTION_KEY=$(openssl rand -base64 32)" > .env   # .env is git-ignored
docker compose up -d --build producer consumer
```

### Check it yourself

```bash
# What Kafka actually stores: tokens, no card numbers
docker exec kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka-1:9092 \
  --topic app-events --from-beginning --property print.headers=true --max-messages 5

# What our consumer sees: decrypted
docker compose logs consumer | grep PaymentAuthorized
```

> **Logs are not masked yet.** The apps log with plain `LoggerFactory` loggers, so the consumer's log output shows decrypted card and personal data. Step 06 adds log masking.

---

# Step 02: Spring Boot producer and consumer

**Goal:** Spring Boot producer and consumer.

For the design and every customized producer and consumer setting, read [docs/step-02-producer-consumer.md](docs/step-02-producer-consumer.md).

The Spring Boot code lives in [springboot/](springboot): a Maven parent project ([springboot/pom.xml](springboot/pom.xml), Spring Boot 4.1, Java 21) with two modules.

| Module | Port | What it does |
|--------|------|--------------|
| [producer](springboot/producer) | 8081 | Web page and REST API that publish JSON to `app-logs`, `app-events` and `app-metrics`. Optional simulator for steady traffic. |
| [consumer](springboot/consumer) | 8082 | `@KafkaListener`s that read each topic in its own consumer group (`log-consumer`, `event-consumer`, `metric-consumer`), 3 threads each. |

How the two connect:
- **Keys:** logs and metrics are keyed by `service`, events by `aggregateId`, so records for the same service or entity stay in order on one partition.
- **JSON types:** the producer writes a short type id (`log`, `event`, `metric`) in the `__TypeId__` header. The consumer maps those ids to its *own* classes, so the two modules share no code.
- **Safe sends:** the producer uses `acks=all` with idempotence on. A send succeeds only once at least 2 replicas have the record.
- **Bad records:** the consumer wraps JSON parsing in `ErrorHandlingDeserializer`. A record it can't read, or that keeps failing, is retried 3 times, logged and skipped, so it never blocks its partition.

## Run it

Start the cluster first (see Step 01 below), then in two terminals:

```bash
cd springboot
mvn -pl consumer spring-boot:run
mvn -pl producer spring-boot:run
# or with generated traffic: one log and one event per second
SIMULATOR_ENABLED=true mvn -pl producer spring-boot:run
```

Both apps connect to `localhost:19092,localhost:29092,localhost:39092`. Override this with `KAFKA_BOOTSTRAP_SERVERS`.

### Or run them in Docker

Each module has its own Dockerfile ([producer](springboot/producer/Dockerfile), [consumer](springboot/consumer/Dockerfile)). Both build from `springboot/`, because a module can't build without the parent POM. Each one runs as a non-root user and splits the jar into layers, so a code change rebuilds only the small application layer.

`docker compose up -d` starts them together with the cluster. They connect to Kafka at `kafka-1:9092,...` and report healthy once `/actuator/health` is up.

The containers are split into two network layers:

| Network | Members | Used for |
|---------|---------|----------|
| `data-layer` | brokers, `kafka-init` | Controller quorum, replication, topic setup. Marked `internal`, so nothing on it can reach the internet. |
| `service-layer` | brokers, `producer`, `consumer`, `kafka-ui` | Services that read from and write to Kafka |

The brokers are the only containers on both networks. Services reach Kafka through `service-layer`, while traffic between the brokers stays on `data-layer`. The published ports (`localhost:19092`, `8080`, `8081`, and so on) still work, because they go through `service-layer`.

When adding a container, put stores like Elasticsearch on `data-layer`. Put anything that runs business logic or a UI on `service-layer`. The ports are the same (8081, 8082), so stop any copies running through Maven first.

```bash
docker compose up -d --build producer consumer   # build and (re)start only the apps
docker compose logs -f consumer                  # watch records arrive
docker build -f springboot/producer/Dockerfile -t producer springboot   # build one image by hand
```

To start the producer with the simulator on, set `SIMULATOR_ENABLED: "true"` in [docker-compose.yml](docker-compose.yml).

## Send messages from the web page

Open **http://localhost:8081**.

1. **Topic:** pick one. The list comes from `GET /api/topics`.
2. **Request body:** a sample for that topic appears. Click *New sample* for another one, or edit it.
3. **Number of requests** and **In parallel:** how many requests to send in total (up to 10,000), and how many are in flight at once (up to 50).
4. **New random data for each request:** when this is on, every request gets fresh sample data. When it's off, every request sends the body exactly as written in the box.
5. **Send:** the page shows sent, acknowledged and failed counts, requests per second, how many records landed on each partition, and the latest responses as `topic-partition@offset`. Press **Stop** to stop early.

## Send messages with curl

```bash
curl -X POST localhost:8081/api/logs -H 'Content-Type: application/json' \
  -d '{"service":"order-service","level":"INFO","message":"Order created"}'
# {"id":"e7ef...","topic":"app-logs","partition":2,"offset":0}

curl -X POST localhost:8081/api/events -H 'Content-Type: application/json' \
  -d '{"type":"OrderPlaced","aggregateId":"order-42","payload":{"amount":99}}'

curl -X POST localhost:8081/api/metrics -H 'Content-Type: application/json' \
  -d '{"service":"order-service","name":"cpu_usage_percent","value":42,"tags":{"instance":"pod-1"}}'
```

The response arrives after the brokers acknowledge the record, and shows where Kafka stored it. The consumer logs each record as `[topic-partition@offset] ...`.

Valid `level` values: `TRACE`, `DEBUG`, `INFO`, `WARN` or `ERROR`. Anything else returns `400`.

## Things to try

- **Bad record:** send something that isn't JSON. The consumer logs `Skipping app-logs-N@M` and carries on.
  ```bash
  echo 'not json' | docker exec -i kafka-1 /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka-1:9092 --topic app-logs
  ```
- **Lag:** stop the consumer while the simulator runs, run `kafka-consumer-groups.sh --describe --group log-consumer` and watch `LAG` grow. Start the consumer again and it catches up from its saved offsets.
- **Uneven partitions:** the simulator's 3 service names hash to only 2 of the 3 partitions of `app-logs`. Few distinct keys means uneven load. On the web page, compare *Records per partition* for `app-events` (50 different order ids) with `app-logs` (5 service names).

---

# Step 01: Kafka cluster and topics

**Goal:** a running Kafka cluster and topics, used from the CLI.

For background and the reasons behind each setting, read [docs/01-kafka-cluster.md](docs/01-kafka-cluster.md).

## What runs

| Service      | What it does                                         | Address                                    |
|--------------|------------------------------------------------------|--------------------------------------------|
| `kafka-1..3` | 3 nodes, each both broker and controller (KRaft)     | containers: `kafka-N:9092`, host: `localhost:19092 / 29092 / 39092` |
| `kafka-init` | Creates the topics once, then exits                   | —                                          |
| `kafka-ui`   | Web UI for browsing topics and messages               | http://localhost:8080                      |

Topics created by [scripts/create-topics.sh](scripts/create-topics.sh). Each has 3 partitions, a replication factor of 3 and keeps data for 7 days:

- `app-logs`
- `app-metrics`
- `app-events`

`min.insync.replicas=2`, so writes with `acks=all` keep working while one broker is down.

## Start and stop

```bash
docker compose up -d            # start
docker compose logs kafka-init  # see which topics were created
docker compose ps -a            # kafka-1..3 healthy, kafka-init "Exited (0)"
docker compose down             # stop, keep data
docker compose down -v          # stop and delete all data
```

## Use it from the CLI

The Kafka CLI tools are inside every broker container at `/opt/kafka/bin`.

> **Git Bash on Windows:** put `MSYS_NO_PATHCONV=1` before `docker exec` commands, or Git Bash rewrites `/opt/...` into a Windows path. PowerShell doesn't need this.

### Topics

```bash
# List
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --list

# Describe: partitions, leader, replicas, in-sync replicas
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --describe --topic app-logs

# Create
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 \
  --create --topic my-topic --partitions 3 --replication-factor 3

# Add partitions (you can increase the count, never decrease it)
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 \
  --alter --topic my-topic --partitions 6

# Delete
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --delete --topic my-topic
```

### Produce and consume

Open two terminals.

```bash
# Terminal 1: producer. Type a line, press Enter. Ctrl+C to quit.
docker exec -it kafka-1 /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka-1:9092 --topic app-events

# Producer with keys: type messages as key:value
docker exec -it kafka-1 /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka-1:9092 --topic app-events \
  --property parse.key=true --property key.separator=:

# Terminal 2: consumer. Read from the start and show key, partition and offset.
docker exec -it kafka-2 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka-2:9092 --topic app-events --from-beginning \
  --property print.key=true --property print.partition=true --property print.offset=true
```

### Consumer groups

```bash
# Consume as part of a group (start this twice to watch partitions get split between the two)
docker exec -it kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka-1:9092 --topic app-events --group demo-group

# List groups, then show offsets and lag for each partition
docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka-1:9092 --list
docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka-1:9092 --describe --group demo-group

# Re-read everything: reset the group to the beginning (stop the group's consumers first)
docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka-1:9092 \
  --group demo-group --topic app-events --reset-offsets --to-earliest --execute
```

### Cluster health

```bash
# KRaft controller quorum: which node is leader, follower lag
docker exec kafka-1 /opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server kafka-1:9092 describe --status

# Partitions where a replica has fallen behind (empty output = healthy)
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --describe --under-replicated-partitions
```

## Try a failover

```bash
docker stop kafka-3
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --describe --topic app-logs
# Partitions that kafka-3 led now have a new leader, and 3 drops out of Isr.
# Producing and consuming still works: 2 of 3 replicas are enough.
docker start kafka-3
# A few seconds later, 3 is back in Isr.
```

## From the host

Tools running on your machine (not in Docker) connect to `localhost:19092,localhost:29092,localhost:39092`.
