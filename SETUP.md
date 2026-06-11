# 🚀 Spring Boot Kafka Event Streaming — Local Setup Guide

This guide walks you through setting up Apache Kafka locally and running both the **Producer** and **Consumer** Spring Boot apps.

---

## 📋 Prerequisites

| Tool | Version | Install |
|------|---------|---------|
| Java | 17+ | `brew install openjdk@17` |
| Maven | 3.8+ | `brew install maven` |
| Apache Kafka | 3.x | See below |

---

## 1️⃣  Install & Start Kafka (macOS — Two Options)

> **Note (Homebrew 2024+):** Modern Homebrew Kafka runs in **KRaft mode** — Zookeeper is no longer needed or installed separately. Just start Kafka directly.

### Option A — Homebrew (Easiest)

```bash
# Install Kafka
brew install kafka

# ⚠️  ONE-TIME STEP: Format the KRaft storage (required on first run)
# This generates a cluster UUID and initialises the metadata log.
# Skip this if Kafka was already working previously.
KAFKA_CLUSTER_ID=$(kafka-storage random-uuid)
kafka-storage format \
  --config /opt/homebrew/etc/kafka/server.properties \
  --cluster-id "$KAFKA_CLUSTER_ID" \
  --standalone

# Start Kafka (KRaft mode — no Zookeeper needed)
brew services start kafka

# Verify Kafka is running
brew services list | grep kafka
```

> If you see **"Invalid cluster.id"** during format, the storage was already initialised with a different ID.
> Run `rm -rf /opt/homebrew/var/lib/kraft-combined-logs/*` first, then repeat the format step above.

### Option B — Manual Download

```bash
# Download Kafka 3.7 (latest stable)
curl -O https://downloads.apache.org/kafka/3.7.0/kafka_2.13-3.7.0.tgz
tar -xzf kafka_2.13-3.7.0.tgz
cd kafka_2.13-3.7.0

# Terminal 1 — Start Zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties

# Terminal 2 — Start Kafka Broker
bin/kafka-server-start.sh config/server.properties
```

> ✅ Kafka runs on `localhost:9092` by default — no config changes needed.

---

## 2️⃣  Create the Kafka Topic

> **Note:** The producer app will **auto-create** the `chat-events` topic on startup.
> But you can also create it manually for more control:

```bash
# For Homebrew install:
kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic chat-events \
  --partitions 3 \
  --replication-factor 1

# For manual install (from kafka directory):
bin/kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --topic chat-events \
  --partitions 3 \
  --replication-factor 1

# Verify topic was created:
kafka-topics --list --bootstrap-server localhost:9092
```

---

## 3️⃣  Run the Producer App (Port 8081)

Open a new terminal:

```bash
cd kafka-producer
mvn spring-boot:run
```

You should see:
```
Started KafkaProducerApplication on port(s): 8081
```

Open the Producer UI: **http://localhost:8081**

---

## 4️⃣  Run the Consumer App (Port 8082)

Open another terminal:

```bash
cd kafka-consumer
mvn spring-boot:run
```

You should see:
```
Started KafkaConsumerApplication on port(s): 8082
```

Open the Consumer UI: **http://localhost:8082**

---

## 5️⃣  Test the Flow

1. Open **http://localhost:8081** (Producer — send messages)
2. Open **http://localhost:8082** (Consumer — watch live feed)
3. Type a message in the Producer UI → click **Send**
4. Watch it appear **instantly** in the Consumer UI 🎉

---

## 🔧 REST API Reference

### Producer (http://localhost:8081)

| Method | Endpoint | Description | Body |
|--------|----------|-------------|------|
| `POST` | `/api/messages` | Send a message to Kafka | `{"sender":"Alice","content":"Hello!","room":"general"}` |
| `GET`  | `/api/messages/history` | Get all sent messages | — |
| `GET`  | `/api/messages/health` | Health check | — |

**Example — send a message via curl:**
```bash
curl -X POST http://localhost:8081/api/messages \
  -H "Content-Type: application/json" \
  -d '{"sender":"Alice","content":"Hello from curl!","room":"general"}'
```

### Consumer (http://localhost:8082)

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET`  | `/api/messages` | Get all received messages (JSON) |
| `GET`  | `/api/messages/stream` | SSE stream (real-time push) |
| `GET`  | `/api/messages/health` | Health check + active connections |

**Example — poll all received messages:**
```bash
curl http://localhost:8082/api/messages | python3 -m json.tool
```

**Example — subscribe to SSE stream in terminal:**
```bash
curl -N http://localhost:8082/api/messages/stream
```

---

## 🛠  Useful Kafka Commands

```bash
# List all topics
kafka-topics --list --bootstrap-server localhost:9092

# Describe the chat-events topic
kafka-topics --describe --topic chat-events --bootstrap-server localhost:9092

# Read all messages from the beginning (console consumer)
kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic chat-events \
  --from-beginning

# Check consumer group lag
kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group chat-consumer-group \
  --describe

# Delete the topic (reset)
kafka-topics --delete --topic chat-events --bootstrap-server localhost:9092
```

---

## 🐛 Troubleshooting

| Problem | Fix |
|---------|-----|
| `Connection refused localhost:9092` | Kafka not running — run `brew services start kafka` |
| `Topic chat-events not found` | Start the producer app first (it auto-creates the topic) |
| Consumer UI shows "Disconnected" | Make sure the consumer app is running on port 8082 |
| Messages not arriving | Check Kafka is running and topic exists: `kafka-topics --list ...` |
| Port 8081/8082 already in use | Kill the process: `lsof -ti:8081 \| xargs kill -9` |
| Deserialization error in consumer logs | Delete and recreate the topic — old messages may have wrong format |

---

## 📁 Project Structure

```
spring-event-streaming/
├── kafka-producer/                         # Port 8081
│   ├── src/main/java/com/eventstreaming/producer/
│   │   ├── KafkaProducerApplication.java   # Entry point
│   │   ├── config/KafkaTopicConfig.java    # Auto-creates topic
│   │   ├── controller/MessageController.java
│   │   ├── service/KafkaProducerService.java
│   │   └── model/ChatMessage.java
│   ├── src/main/resources/
│   │   ├── application.yml
│   │   └── static/index.html               # Producer Chat UI
│   └── pom.xml
│
├── kafka-consumer/                         # Port 8082
│   ├── src/main/java/com/eventstreaming/consumer/
│   │   ├── KafkaConsumerApplication.java   # Entry point
│   │   ├── controller/MessageStreamController.java
│   │   ├── service/KafkaConsumerService.java
│   │   └── model/ChatMessage.java
│   ├── src/main/resources/
│   │   ├── application.yml
│   │   └── static/index.html               # Consumer Live Feed UI
│   └── pom.xml
│
└── SETUP.md                                # This file
```

---

## ⚡ Quick Start (All-in-One)

If you just want to get everything running fast:

```bash
# Terminal 1 — Start Kafka (KRaft mode, no Zookeeper needed)
brew services start kafka

# Terminal 2 — Start Producer
cd kafka-producer && mvn spring-boot:run

# Terminal 3 — Start Consumer
cd kafka-consumer && mvn spring-boot:run

# Then open:
# http://localhost:8081  →  Producer (send messages)
# http://localhost:8082  →  Consumer (receive messages live)
```

---

## 🛑 Stop Everything

### Stop the Spring Boot Apps

If running in terminal (`mvn spring-boot:run`), press **Ctrl+C** in each terminal.

To kill by port if needed:
```bash
# Stop Producer (port 8081)
lsof -ti:8081 | xargs kill -9

# Stop Consumer (port 8082)
lsof -ti:8082 | xargs kill -9
```

### Stop Kafka

```bash
# Stop the Kafka background service
brew services stop kafka

# Verify it stopped
brew services list | grep kafka
# Should show: kafka  stopped
```

> **Note:** `brew services stop kafka` stops Kafka immediately **and** disables auto-start on login.
> Next time you need it, just run `brew services start kafka` again.

### Run Kafka in Terminal (session-only — dies on Ctrl+C)

If you don't want Kafka running permanently in the background, use this instead of `brew services start`:

```bash
# Start Kafka tied to this terminal (Ctrl+C to stop)
/opt/homebrew/opt/kafka/bin/kafka-server-start \
  /opt/homebrew/etc/kafka/server.properties
```

This is useful for quick testing — Kafka dies automatically when you close the terminal.
