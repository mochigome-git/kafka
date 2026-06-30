# Kafka on EC2 (apache/kafka 4.1.0, KRaft + TLS)

A 2-broker Apache Kafka cluster on two EC2 instances, KRaft mode (no ZooKeeper),
running the official `apache/kafka:4.1.0` image with TLS on the client + inter-broker listener.

```
kafka-ec2/
├── docker-compose.yml          # per-node broker (deploy on each EC2)
├── .env                        # passwords + per-node NODE_ID / KAFKA_DOMAIN  (git-ignored)
├── scripts/
│   └── generate-certs.sh       # builds the full CA + keystore/truststore chain
└── secrets/                    # generated certs (mounted into the broker, git-ignored)
    └── client-ssl.properties
```

---

## 0. Requirements

The cert script uses `keytool`, which ships with the JDK. Install a JDK on whatever
machine you run `generate-certs.sh` on:

```bash
# Amazon Linux 2023
sudo dnf install -y java-21-amazon-corretto-headless

# Amazon Linux 2
sudo yum install -y java-17-amazon-corretto-headless

# Ubuntu / Debian
sudo apt update && sudo apt install -y default-jdk

# macOS
brew install openjdk
```

Verify, then you're ready:

```bash
keytool -help >/dev/null && echo "keytool OK"
```

Also required on each EC2: **Docker** + **Docker Compose** (to run the broker) and
**openssl** (already present on Amazon Linux). You do _not_ need the JDK on the EC2
hosts themselves — only where you generate certs. (Alternatively, the
`apache/kafka:4.1.0` image already bundles `keytool`, so you can run the cert script's
Java steps inside a throwaway container instead of installing a JDK.)

---

## 1. Generate certificates

The script reads its settings from `.env`. Set the passwords and the SAN list there:

```bash
# .env  (shared by the cert script AND docker-compose)
STORE_PASS=patapon
KEY_PASS=patapon
TRUST_PASS=patapon
CA_PASS=patapon
# Every hostname clients/brokers connect to MUST be listed here:
SANS="DNS:kafka1.pixelsofts.com,DNS:kafka2.pixelsofts.com,DNS:kafka.pixelsofts.com,DNS:localhost,IP:127.0.0.1"
```

Run it:

```bash
cd scripts
OUT_DIR=../secrets ./generate-certs.sh
```

Outputs (in `secrets/`):

| File                                                                | Purpose                                                                                 |
| ------------------------------------------------------------------- | --------------------------------------------------------------------------------------- |
| `ca.key`                                                            | CA private key — keep safe, do NOT copy to the brokers                                  |
| `ca.crt`                                                            | CA root cert — the trust anchor for clients (PEM)                                       |
| `ca.srl`                                                            | CA serial file                                                                          |
| `kafka.csr`                                                         | broker signing request                                                                  |
| `kafka.ext`                                                         | SAN list used to sign the broker cert                                                   |
| `kafka.crt`                                                         | broker cert, signed by the CA                                                           |
| `kafka.keystore.jks`                                                | broker private key + signed cert + CA                                                   |
| `kafka.truststore.jks`                                              | trust anchor for the broker                                                             |
| `kafka_keystore_creds`, `kafka_key_creds`, `kafka_truststore_creds` | password files — **only needed by Confluent images, NOT apache/kafka** (see note below) |

> **SANs are the #1 cause of TLS failures.** With inter-broker on SSL, each broker
> connects to the _other_ broker's advertised SSL name and verifies the hostname
> against the cert SAN. So the SAN list MUST contain both `kafka1.pixelsofts.com` and
> `kafka2.pixelsofts.com` (plus any name external clients use). One cert with all
> names in the SAN list is copied to both EC2s.

Copy `secrets/` to each EC2 (e.g. `/home/ec2-user/kafka/certs`), which the compose
mounts read-only into the container at `/etc/kafka/secrets`.

> **Note on the `*_creds` files:** those are a Confluent-image convention. The official
> `apache/kafka` image does NOT read credential files — it takes the password directly
> via `KAFKA_SSL_*_PASSWORD` env vars (wired from `.env` in the compose below). You can
> ignore or delete the generated `*_creds` files; they're harmless but unused.

---

## 2. Broker config (docker-compose.yml)

Deploy the SAME file on both EC2s; only `.env` differs per node. Note the SSL block
uses `*_PASSWORD` (apache style), pulled from `.env` — **not** the Confluent
`*_CREDENTIALS` files.

```yaml
networks:
  kafkanet:
    driver: bridge
services:
  kafka:
    image: apache/kafka:4.1.0
    container_name: kafka
    environment:
      # Cluster Basics
      KAFKA_NODE_ID: ${NODE_ID}
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka1.pixelsofts.com:9094,2@kafka2.pixelsofts.com:9094
      CLUSTER_ID: "PASTE-SAME-UUID-ON-BOTH-NODES"
      # Listeners
      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9094,SSL://:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092,SSL://${KAFKA_DOMAIN}:9093
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,SSL:SSL
      KAFKA_INTER_BROKER_LISTENER_NAME: SSL
      # Broker config
      KAFKA_LOG_DIRS: /var/lib/kafka/data
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 2
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 2
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_MIN_INSYNC_REPLICAS: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_NUM_PARTITIONS: 3
      # SSL  (apache/kafka style: direct passwords from .env, no creds files)
      KAFKA_SSL_KEYSTORE_LOCATION: /etc/kafka/secrets/kafka.keystore.jks
      KAFKA_SSL_KEYSTORE_PASSWORD: ${STORE_PASS}
      KAFKA_SSL_KEY_PASSWORD: ${KEY_PASS}
      KAFKA_SSL_TRUSTSTORE_LOCATION: /etc/kafka/secrets/kafka.truststore.jks
      KAFKA_SSL_TRUSTSTORE_PASSWORD: ${TRUST_PASS}
      KAFKA_SSL_CLIENT_AUTH: none
      KAFKA_SSL_KEYSTORE_TYPE: JKS
      KAFKA_SSL_TRUSTSTORE_TYPE: JKS
    healthcheck:
      test:
        [
          "CMD",
          "/opt/kafka/bin/kafka-topics.sh",
          "--bootstrap-server",
          "localhost:9092",
          "--list",
        ]
      interval: 10s
      timeout: 10s
      retries: 10
      start_period: 40s
    volumes:
      - kafka_data:/var/lib/kafka/data
      - /home/ec2-user/kafka/certs:/etc/kafka/secrets:ro
    ports:
      - 9092:9092 # PLAINTEXT internal (same-host clients)
      - 9093:9093 # SSL external + inter-broker
      - 9094:9094 # CONTROLLER — restrict to the 2 instances in your security group
    networks:
      - kafkanet
    restart: unless-stopped
  postgres:
    image: postgres:16
    environment:
      POSTGRES_USER: app
      POSTGRES_PASSWORD: app
      POSTGRES_DB: appdb
    ports:
      - 5432:5432
    volumes:
      - pgdata:/var/lib/postgresql/data
    networks:
      - kafkanet
volumes:
  pgdata:
  kafka_data:
```

---

## 3. Deploy

Generate the cluster UUID ONCE and paste the same value into `CLUSTER_ID` on both nodes:

```bash
docker run --rm apache/kafka:4.1.0 /opt/kafka/bin/kafka-storage.sh random-uuid
```

`.env` per box (only these two differ between nodes; passwords/SANS stay the same):

```bash
# EC2 #1
NODE_ID=1
KAFKA_DOMAIN=kafka1.pixelsofts.com

# EC2 #2
NODE_ID=2
KAFKA_DOMAIN=kafka2.pixelsofts.com
```

Then on each box:

```bash
docker compose up -d
docker compose logs -f kafka
```

For a single broker, set `KAFKA_CONTROLLER_QUORUM_VOTERS` to just
`1@kafka1.pixelsofts.com:9094` and drop the replication factors to `1`.

---

## 4. Scaling 1 -> 2 EC2

The per-node config lives in `.env`; the cluster-wide config is already in the compose.

1. **`.env` per box** — node 1 = `NODE_ID=1` / `kafka1...`, node 2 = `NODE_ID=2` / `kafka2...`.
2. **Same `CLUSTER_ID`** on both (generated once, above).
3. **Quorum voters** list both nodes (already set).
4. **Inter-broker on SSL** — `kafka:9092` (PLAINTEXT) is container-local and can't cross
   hosts, so inter-broker replication uses the SSL listener (encrypted + reachable).
5. **Replication factor 2** — so internal topics survive losing one broker.
6. **Security group** — open `9093` and `9094` between the two instances, and `9093`
   from wherever your clients live. Ideally both EC2s in the same VPC.

> **Quorum caveat:** KRaft elects by majority. With 2 controllers, majority is 2, so
> losing one node stalls controller elections until it returns (data on the survivor is
> still readable). For real controller fault tolerance, run 3 nodes (odd number).

---

## 5. Route 53 — and why NOT an ALB

**An Application Load Balancer cannot front Kafka.** ALB is Layer 7 (HTTP/HTTPS/gRPC
only); Kafka is a raw TCP binary protocol. Also, a Kafka client first hits a bootstrap
address, the broker replies with its `advertised.listeners` address, and the client then
connects DIRECTLY to that specific broker — so a round-robin LB breaks it.

### Option A — Direct per-broker DNS (recommended for 2 brokers)

No load balancer. One Route 53 record per broker:

| Record                  | Type | Value             |
| ----------------------- | ---- | ----------------- |
| `kafka1.pixelsofts.com` | A    | EC2 #1 Elastic IP |
| `kafka2.pixelsofts.com` | A    | EC2 #2 Elastic IP |

Use Elastic IPs so the address survives restarts. Clients bootstrap to both and fail
over automatically:

```
bootstrap.servers=kafka1.pixelsofts.com:9093,kafka2.pixelsofts.com:9093
```

### Option B — Network Load Balancer (L4) if you need one entry point

Use an NLB (TCP, Layer 4) — never an ALB. Because of advertised listeners, give each
broker its OWN port through the NLB:

| NLB listener | Target   | Advertises as               |
| ------------ | -------- | --------------------------- |
| TCP `9093`   | broker 1 | `kafka.pixelsofts.com:9093` |
| TCP `9193`   | broker 2 | `kafka.pixelsofts.com:9193` |

Each broker's SSL advertised listener becomes `kafka.pixelsofts.com:<its-port>`, the
NLB name goes in the cert SANs, and Route 53 `kafka.pixelsofts.com` is an ALIAS to the
NLB. Option A is simpler; use B only when one entry point is a hard requirement.

---

## 6. Create topics

The SSL listener needs `--command-config`. Put `client-ssl.properties` in `secrets/` so
it mounts to `/etc/kafka/secrets/`:

```properties
# secrets/client-ssl.properties
security.protocol=SSL
ssl.truststore.location=/etc/kafka/secrets/kafka.truststore.jks
ssl.truststore.password=patapon
ssl.endpoint.identification.algorithm=https
```

Commands (note: apache image uses the `.sh` scripts under `/opt/kafka/bin`):

```bash
# Create (RF 2 spreads across both brokers)
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9093 \
  --command-config /etc/kafka/secrets/client-ssl.properties \
  --create --topic raw-metrics \
  --partitions 6 --replication-factor 2 \
  --config min.insync.replicas=1 --config retention.ms=604800000

# List
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9093 \
  --list \
  --command-config /etc/kafka/secrets/client-ssl.properties

# count what's actually in the topic (per-partition latest offsets)
docker exec kafka /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server localhost:9093 \
  --topic gim_devices_payload \
  --command-config /etc/kafka/secrets/client-ssl.properties

# Describe
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9093 \
  --command-config /etc/kafka/secrets/client-ssl.properties \
  --describe --topic raw-metrics

# Increase partitions (cannot decrease)
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9093 \
  --command-config /etc/kafka/secrets/client-ssl.properties \
  --alter --topic raw-metrics --partitions 12
```

Smoke test:

```bash
# Producer
docker exec -it kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9093 \
  --producer.config /etc/kafka/secrets/client-ssl.properties --topic raw-metrics

# Consumer (another shell)
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9093 \
  --consumer.config /etc/kafka/secrets/client-ssl.properties \
  --topic gim_devices_payload --from-beginning

# does data exist / is it growing?
docker exec kafka /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server localhost:9092 \
  --topic gim_devices_payload

# watch live
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic gim_devices_payload --from-beginning

```

For a Go client (confluent-kafka-go / librdkafka), use the PEM CA, not the JKS:

```
bootstrap.servers = kafka1.pixelsofts.com:9093,kafka2.pixelsofts.com:9093
security.protocol  = SSL
ssl.ca.location    = /path/to/ca.crt
```

---

## Notes

- `min.insync.replicas=1` favours availability; use `2` with producer `acks=all` for
  stronger durability (produce blocks if one broker is down).
- The local containers (postgres, etc.) on each host reach Kafka via the PLAINTEXT
  listener at `kafka:9092` inside the docker network — unchanged.
- Keep `.env`, `*.jks`, and `ca.key` out of git.
- To enforce mutual TLS, set `KAFKA_SSL_CLIENT_AUTH: required` and issue a client cert
  signed by the same CA.
