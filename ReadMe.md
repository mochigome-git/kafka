# Kafka on EC2 (KRaft + TLS)

A 2-broker Apache Kafka cluster running on two EC2 instances in KRaft mode (no ZooKeeper), with TLS encryption on the client and inter-broker listener.

```
kafka-ec2/
├── docker-compose.yml          # per-node broker (deploy on each EC2)
├── .env.example                # NODE_ID / ADVERTISED_HOST / CLUSTER_ID per node
├── scripts/
│   └── generate-certs.sh       # builds the full CA + keystore/truststore chain
└── secrets/                    # generated certs live here (git-ignore this!)
    └── client-ssl.properties
```

---

## 1. Generate certificates

Run this **once**, on your workstation. It produces every artifact the broker and clients need.

```bash
cd scripts

# Edit the SANs to match exactly the hostnames clients will connect to.
SANS="DNS:kafka1.general-my.com,DNS:kafka2.general-my.com,DNS:kafka.general-my.com,DNS:localhost,IP:127.0.0.1" \
STORE_PASS="your-keystore-pass" \
TRUST_PASS="your-truststore-pass" \
CA_PASS="your-ca-pass" \
OUT_DIR=../secrets \
./generate-certs.sh
```

Outputs (in `secrets/`):

| File                                                                | Purpose                                                      |
| ------------------------------------------------------------------- | ------------------------------------------------------------ |
| `ca.key`                                                            | CA private key (keep offline / safe)                         |
| `ca.crt`                                                            | CA root cert — distribute to **clients** as the trust anchor |
| `ca.srl`                                                            | CA serial file                                               |
| `kafka.csr`                                                         | broker signing request                                       |
| `kafka.ext`                                                         | SAN list used to sign the broker cert                        |
| `kafka.crt`                                                         | broker cert, signed by the CA                                |
| `kafka.keystore.jks`                                                | broker private key + signed cert + CA                        |
| `kafka.truststore.jks`                                              | trust anchor for the broker                                  |
| `kafka_keystore_creds`, `kafka_key_creds`, `kafka_truststore_creds` | password files read by the broker                            |

> **SANs are the #1 source of TLS failures.** Hostname verification (`ssl...algorithm=https`) checks the address in `advertised.listeners` against the cert SAN. Every hostname a client may use — each broker's DNS name **and** the NLB name if you add one — must be in `SANS`. Both brokers can share one cert because all names are baked into a single SAN list.

The same `secrets/` folder is copied to **both** EC2 instances (the cert covers both broker hostnames).

---

## 2. Deploy a single broker (your current setup)

On the EC2 box:

```bash
# Generate the cluster UUID once, reuse it on every node:
docker run --rm confluentinc/cp-kafka:7.7.1 kafka-storage random-uuid

# Create .env from .env.example with NODE_ID=1, ADVERTISED_HOST=kafka1.general-my.com, CLUSTER_ID=<uuid>
cp .env.example .env && nano .env

# Make sure secrets/ is present, then:
docker compose up -d
docker compose logs -f kafka
```

For a true single-node cluster, set `KAFKA_CONTROLLER_QUORUM_VOTERS` to just `1@kafka1.general-my.com:9093` and drop the replication factors to `1`.

---

## 3. Scaling to 2 EC2 instances

The change is small. Per-broker config lives in `.env`; the cluster-wide config is already in the compose file.

**What changes vs. 1 node:**

1. **`.env` per box** — node 1 gets `NODE_ID=1` / `kafka1...`, node 2 gets `NODE_ID=2` / `kafka2...`. `CLUSTER_ID` is **identical** on both.
2. **Quorum voters** — `KAFKA_CONTROLLER_QUORUM_VOTERS` already lists both:
   `1@kafka1.general-my.com:9093,2@kafka2.general-my.com:9093`
3. **Replication factor 2** — already set, so partitions survive losing one broker.
4. **Security group** — open `9092` (clients/inter-broker, TLS) and `9093` (controller) **between the two instances**, and `9092` from wherever your producers/consumers live.

> **Quorum caveat:** KRaft elects a controller by majority. With 2 controllers, majority is 2 — so losing one node stalls controller elections until it returns (data on the surviving broker is still readable). For real controller fault tolerance you need an **odd** number — run **3** brokers, or add a 3rd small instance as a dedicated controller. Two brokers is fine for throughput and data redundancy; just know the trade-off.

Bring up node 1 first, then node 2:

```bash
# on each EC2, after writing its own .env:
docker compose up -d
```

---

## 4. Route 53 — and why **not** an ALB

**An Application Load Balancer cannot front Kafka.** ALB is Layer 7 (HTTP / HTTPS / gRPC only); Kafka is a raw TCP binary protocol, so an ALB will not pass the traffic. There's also a protocol reason a normal load balancer doesn't fit: a client first hits a _bootstrap_ address, the broker replies with its `advertised.listeners` address, and the client then connects **directly** to that specific broker. So whatever you advertise must be independently routable to that one broker — round-robining across brokers breaks it.

You have two correct options:

### Option A — Direct per-broker DNS (recommended for 2 brokers)

No load balancer. One Route 53 record per broker:

| Record                  | Type | Value                             |
| ----------------------- | ---- | --------------------------------- |
| `kafka1.general-my.com` | A    | EC2 #1 Elastic IP (or private IP) |
| `kafka2.general-my.com` | A    | EC2 #2 Elastic IP (or private IP) |

- Use **Elastic IPs** so the address survives instance restarts.
- Clients in the same VPC → use a **private hosted zone** + private IPs.
- HA is built into the client: it bootstraps to the list and fails over automatically.

```
bootstrap.servers=kafka1.general-my.com:9092,kafka2.general-my.com:9092
```

`advertised.listeners` is each broker's own DNS name — this is already wired via `ADVERTISED_HOST` in `.env`.

### Option B — Network Load Balancer (L4) if you need one entry point

If you want a single stable hostname (e.g. clients can't be given two), use an **NLB (TCP, Layer 4)** — not an ALB. Because of advertised listeners, you must give **each broker its own port** through the NLB:

| NLB listener | Target group  | Advertises as               |
| ------------ | ------------- | --------------------------- |
| TCP `9092`   | broker 1 only | `kafka.general-my.com:9092` |
| TCP `9192`   | broker 2 only | `kafka.general-my.com:9192` |

- Each broker's `KAFKA_ADVERTISED_LISTENERS` becomes `SSL://kafka.general-my.com:<its-port>`, and each broker also listens on that distinct port.
- Add the NLB hostname (`kafka.general-my.com`) to the cert SANs (it already is in the default `SANS`).
- Route 53 `kafka.general-my.com` → **ALIAS** record to the NLB.

Option A is simpler and fine for two brokers. Reach for B only when a single entry point is a hard requirement.

---

## 5. Create topics

The broker requires TLS, so CLI tools need `--command-config` pointing at the client properties (`secrets/client-ssl.properties` mounts to `/etc/kafka/secrets/`).

```bash
# Create a topic (RF 2 spreads it across both brokers)
docker exec -it kafka kafka-topics \
  --bootstrap-server kafka1.general-my.com:9092 \
  --command-config /etc/kafka/secrets/client-ssl.properties \
  --create --topic raw-metrics \
  --partitions 6 --replication-factor 2 \
  --config min.insync.replicas=1 \
  --config retention.ms=604800000

# List
docker exec -it kafka kafka-topics \
  --bootstrap-server kafka1.general-my.com:9092 \
  --command-config /etc/kafka/secrets/client-ssl.properties --list

# Describe (check leader / ISR distribution)
docker exec -it kafka kafka-topics \
  --bootstrap-server kafka1.general-my.com:9092 \
  --command-config /etc/kafka/secrets/client-ssl.properties \
  --describe --topic raw-metrics

# Increase partitions (cannot decrease)
docker exec -it kafka kafka-topics \
  --bootstrap-server kafka1.general-my.com:9092 \
  --command-config /etc/kafka/secrets/client-ssl.properties \
  --alter --topic raw-metrics --partitions 12
```

Quick smoke test:

```bash
# Producer
docker exec -it kafka kafka-console-producer \
  --bootstrap-server kafka1.general-my.com:9092 \
  --producer.config /etc/kafka/secrets/client-ssl.properties \
  --topic raw-metrics

# Consumer (another shell)
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server kafka1.general-my.com:9092 \
  --consumer.config /etc/kafka/secrets/client-ssl.properties \
  --topic raw-metrics --from-beginning
```

For a Go client (`confluent-kafka-go` / librdkafka), point at the **PEM** CA rather than the JKS truststore:

```
bootstrap.servers = kafka1.general-my.com:9092,kafka2.general-my.com:9092
security.protocol  = SSL
ssl.ca.location    = /path/to/ca.crt
```

---

## Notes

- `min.insync.replicas=1` favours availability; set it to `2` with producer `acks=all` for stronger durability (a produce will then block if one broker is down).
- Rotate `ca.key` and the keystore passwords for production; the script defaults are placeholders.
- Add `secrets/` to `.gitignore` — never commit keys or `*.jks`.
- To enforce mutual TLS, set `KAFKA_SSL_CLIENT_AUTH: required` and issue a client keystore (same flow as the broker, signed by the same CA).
