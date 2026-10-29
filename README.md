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
