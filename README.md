# ELK + Kafka + Grafana

A log and metrics pipeline built one step at a time. Each step is a branch.

| Step | Branch | Goal |
|------|--------|------|
| 01 | `step-01-kafka-init` | A running Kafka cluster and topics, used from the CLI |
| 02 | `step-02-producer-consumer` | Spring Boot producer and consumer |
| 03 | `step-03-protect-kafka-data` | No card or personal data readable in Kafka |

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
