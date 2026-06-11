# ⚡ Spring Boot Kafka Event Streaming

> Real-time event streaming with Apache Kafka — producer, consumer, live UI, and production-grade retry pattern.

![Java](https://img.shields.io/badge/Java-21-orange?style=flat-square&logo=openjdk)
![Spring Boot](https://img.shields.io/badge/Spring_Boot-4.1.0-brightgreen?style=flat-square&logo=springboot)
![Kafka](https://img.shields.io/badge/Apache_Kafka-3.x-black?style=flat-square&logo=apachekafka)
![License](https://img.shields.io/badge/License-MIT-blue?style=flat-square)

---

## What This Is

Two independent Spring Boot apps connected via a local Kafka broker.
Send a message in one browser tab — watch it arrive in another in under a second.

```
┌─────────────────┐        Kafka         ┌─────────────────┐
│  kafka-producer │  ──── chat-events ──► │  kafka-consumer │
│   localhost:8081│                       │   localhost:8082│
│   💬 Chat UI    │                       │   📡 Live Feed  │
└─────────────────┘                       └─────────────────┘
```

Messages are pushed to the browser in real-time via **Server-Sent Events (SSE)** — no polling, no WebSocket setup.

---

## Quick Start

```bash
# 1 — Format Kafka storage (one-time, first run only)
KAFKA_CLUSTER_ID=$(kafka-storage random-uuid)
kafka-storage format \
  --config /opt/homebrew/etc/kafka/server.properties \
  --cluster-id "$KAFKA_CLUSTER_ID" --standalone

# 2 — Start Kafka (KRaft mode, no Zookeeper needed)
brew services start kafka

# 3 — Start Producer
cd kafka-producer && mvn spring-boot:run

# 4 — Start Consumer
cd kafka-consumer && mvn spring-boot:run
```

Open **http://localhost:8081** → type a message → watch it appear at **http://localhost:8082** ✨

> Full setup guide: [`SETUP.md`](SETUP.md)

---

## REST API

**Producer** `localhost:8081`

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/api/messages` | Publish message to Kafka |
| `GET` | `/api/messages/history` | Sent message history |

**Consumer** `localhost:8082`

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/api/messages/stream` | SSE real-time stream |
| `GET` | `/api/messages` | All received messages |

```bash
# Send a message via curl
curl -X POST http://localhost:8081/api/messages \
  -H "Content-Type: application/json" \
  -d '{"sender":"Samar","content":"Hello Kafka!","room":"general"}'
```

---

## Production Pattern (EKS + MSK + SQS)

This repo also documents a production-grade pattern for consuming from an external Kafka topic, calling a downstream REST API with **15-minute interval retries**, and routing permanently failed messages to **AWS SQS** after 5 attempts.

```
External Kafka → Consumer Pod (EKS) → REST API
                      │
                      ├── ✅ Success → commit offset
                      └── ❌ Fail → retry every 15 min × 4
                                        └── 5th fail → AWS SQS
```

> See [`KAFKA_RETRY_SQS_PATTERN.md`](KAFKA_RETRY_SQS_PATTERN.md) for full code and architecture.

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Language | Java 21 |
| Framework | Spring Boot 4.1.0 |
| Messaging | Apache Kafka (KRaft) |
| Real-time | Server-Sent Events (SSE) |
| Build | Maven |
| Local broker | Homebrew Kafka |
| Cloud broker | AWS MSK (config only) |
| Dead letter | AWS SQS |

---

## Project Structure

```
spring-event-streaming/
├── kafka-producer/          # Port 8081 — sends messages
├── kafka-consumer/          # Port 8082 — receives messages via SSE
├── SETUP.md                 # Local Kafka setup guide
└── KAFKA_RETRY_SQS_PATTERN.md  # Production retry + SQS pattern
```

---

## Author

**Samar Dash**
GitHub: [@12dit152](https://github.com/12dit152)
