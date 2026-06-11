# Kafka Consumer — Retry & SQS Dead Letter Pattern

This document covers the **production-grade pattern** for consuming from an external Kafka topic,
calling a downstream REST API, retrying on failure every 15 minutes, and pushing permanently
failed messages to SQS after 5 attempts.

---

## Architecture Overview

```
External Team's Kafka Broker
         │
         │  (you are the consumer)
         ▼
┌─────────────────────────────────────────────────────────────┐
│                    YOUR EKS PODS                            │
│                                                              │
│  [main-topic]                                               │
│       │                                                      │
│       ▼                                                      │
│  KafkaConsumerService.consume()                             │
│       │                                                      │
│       ├──► REST API call ──► ✅ Success → commit offset     │
│       │                                                      │
│       └──► ❌ Fail                                           │
│               │                                              │
│               ▼                                              │
│  [main-topic-retry-0]  (wait 15 mins)                       │
│       │                                                      │
│       ├──► REST API call ──► ✅ Success → commit offset     │
│       │                                                      │
│       └──► ❌ Fail  (repeat up to 4 retries)                │
│               │                                              │
│               ▼                                              │
│  @DltHandler fires after 5th failure                        │
│       │                                                      │
│       └──► AWS SQS ──► Alert / Manual Review                │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

---

## Topics Involved

| Topic | Purpose | Created by |
|-------|---------|-----------|
| `main-topic` | Original messages from external producer | External team |
| `main-topic-retry-0` | Failed messages waiting 15 mins before retry | Spring Kafka auto-creates |
| `main-topic-dlt` | Parking topic (unused — we go to SQS instead) | Spring Kafka auto-creates (optional) |
| AWS SQS Queue | Final alert destination after 5 failures | Your infrastructure |

---

## EKS Multi-Pod Behaviour

```
Kafka Partitions (12)          EKS Pods (auto-scaled, max 12)
──────────────────────         ──────────────────────────────
P0  ───────────────────►  Pod A
P1  ───────────────────►  Pod A   (multiple partitions per pod at low scale)
P2  ───────────────────►  Pod B
...

Rules:
  ✅ Each partition → exactly ONE pod at any time (consumer group guarantee)
  ✅ Pod dies       → Kafka rebalances partitions to remaining pods
  ✅ Pod scales up  → Kafka rebalances, new pod gets assigned partitions
  ⚠️  Pods > Partitions → extra pods sit IDLE (not harmful, not consuming)

Recommendation: set topic partitions = HPA maxReplicas (e.g. both = 12)
```

---

## Required Dependencies (`pom.xml`)

```xml
<!-- Spring Kafka -->
<dependency>
    <groupId>org.springframework.kafka</groupId>
    <artifactId>spring-kafka</artifactId>
</dependency>

<!-- AWS SQS (v2 SDK) -->
<dependency>
    <groupId>software.amazon.awssdk</groupId>
    <artifactId>sqs</artifactId>
    <version>2.25.0</version>
</dependency>

<!-- Spring Retry (required for @RetryableTopic) -->
<dependency>
    <groupId>org.springframework.retry</groupId>
    <artifactId>spring-retry</artifactId>
</dependency>
```

---

## application.yml

```yaml
server:
  port: 8080

spring:
  application:
    name: kafka-consumer-service

  kafka:
    bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}
    consumer:
      group-id: your-consumer-group
      auto-offset-reset: earliest
      enable-auto-commit: false          # CRITICAL — manual commit only
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.JsonDeserializer
      properties:
        spring.json.trusted.packages: "*"
        spring.json.value.default.type: com.yourcompany.model.YourEvent
    listener:
      ack-mode: RECORD                   # commit after each record processed

# MSK (production) — add these for AWS MSK with IAM auth
#   properties:
#     security.protocol: SASL_SSL
#     sasl.mechanism: AWS_MSK_IAM
#     sasl.jaas.config: software.amazon.msk.auth.iam.IAMLoginModule required;
#     sasl.client.callback.handler.class: software.amazon.msk.auth.iam.IAMClientCallbackHandler

app:
  kafka:
    topic: main-topic
    retry-delay-ms: 900000             # 15 minutes
    max-attempts: 5                    # 1 original + 4 retries
  downstream:
    api-url: ${DOWNSTREAM_API_URL:http://your-api/endpoint}
  sqs:
    queue-url: ${SQS_QUEUE_URL:https://sqs.eu-west-1.amazonaws.com/123/your-queue}
```

---

## Event Model

```java
package com.yourcompany.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Data;
import java.time.LocalDateTime;

@Data
public class YourEvent {
    private String id;
    private String payload;
    private String source;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime eventTimestamp;
}
```

---

## SQS Config Bean

```java
package com.yourcompany.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;

@Configuration
public class SqsConfig {

    /**
     * SqsClient uses the default AWS credential chain:
     *   1. EKS Pod IAM role (recommended — no keys in code)
     *   2. Environment variables AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY
     *   3. ~/.aws/credentials (local dev)
     */
    @Bean
    public SqsClient sqsClient() {
        return SqsClient.builder()
                .region(Region.EU_WEST_1)  // change to your region
                .build();
    }
}
```

---

## Downstream REST API Client

```java
package com.yourcompany.client;

import com.yourcompany.model.YourEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.RestTemplate;

@Slf4j
@Component
@RequiredArgsConstructor
public class DownstreamApiClient {

    private final RestTemplate restTemplate;

    @Value("${app.downstream.api-url}")
    private String apiUrl;

    /**
     * Calls the downstream REST API with the event payload.
     * Throws an exception on failure — this triggers Spring Kafka retry.
     *
     * Only throw for RETRYABLE errors (5xx, timeouts).
     * For 4xx (bad data), you may want to skip directly to DLT/SQS.
     */
    public void send(YourEvent event) {
        log.info("Calling downstream API for event id={}", event.getId());

        ResponseEntity<String> response = restTemplate.postForEntity(
                apiUrl,
                event,
                String.class
        );

        if (!response.getStatusCode().is2xxSuccessful()) {
            throw new HttpServerErrorException(
                    response.getStatusCode(),
                    "Downstream API returned: " + response.getStatusCode()
            );
        }

        log.info("Downstream API success for event id={}", event.getId());
    }
}
```

---

## Main Consumer Service — with `@RetryableTopic`

```java
package com.yourcompany.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.yourcompany.client.DownstreamApiClient;
import com.yourcompany.model.YourEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.DltStrategy;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaConsumerService {

    private final DownstreamApiClient downstreamApiClient;
    private final SqsClient sqsClient;
    private final ObjectMapper objectMapper;

    @Value("${app.sqs.queue-url}")
    private String sqsQueueUrl;

    /**
     * Main consumer + retry configuration.
     *
     * attempts = "5"  →  1 original attempt + 4 retries = 5 total
     * delay    = 900000ms = 15 minutes between each retry
     * multiplier = 1.0  →  fixed 15-min interval (not exponential)
     *
     * Spring Kafka auto-creates:
     *   main-topic-retry-0   (messages wait here for 15 mins before retry)
     *   main-topic-dlt       (we redirect this to SQS via @DltHandler)
     */
    @RetryableTopic(
            attempts = "5",
            backoff = @Backoff(delay = 900_000, multiplier = 1.0),
            dltStrategy = DltStrategy.FAIL_ON_ERROR,
            topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
            autoCreateTopics = "true",
            retryTopicSuffix = "-retry",
            dltTopicSuffix = "-dlt"
    )
    @KafkaListener(
            topics = "${app.kafka.topic}",
            groupId = "${spring.kafka.consumer.group-id}"
    )
    public void consume(
            YourEvent event,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset
    ) {
        log.info("Received event id={} from topic={} partition={} offset={}",
                event.getId(), topic, partition, offset);

        // Call downstream REST API — throws on failure → triggers retry
        downstreamApiClient.send(event);

        log.info("Successfully processed event id={}", event.getId());
    }

    /**
     * Called automatically by Spring Kafka after ALL 5 attempts have failed.
     * At this point we give up on Kafka retries and push the event to SQS
     * for the team to alert on and investigate manually.
     */
    @DltHandler
    public void handleDeadLetter(
            YourEvent event,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.EXCEPTION_MESSAGE) String errorMessage
    ) {
        log.error("Event id={} exhausted all retries. Pushing to SQS. Error: {}",
                event.getId(), errorMessage);

        try {
            String messageBody = objectMapper.writeValueAsString(event);

            SendMessageRequest request = SendMessageRequest.builder()
                    .queueUrl(sqsQueueUrl)
                    .messageBody(messageBody)
                    .messageGroupId(event.getId())       // for FIFO queues
                    .messageDeduplicationId(event.getId()) // for FIFO dedup
                    .build();

            sqsClient.sendMessage(request);

            log.info("Event id={} successfully pushed to SQS queue", event.getId());

        } catch (Exception e) {
            // If SQS push also fails, log as CRITICAL — needs external alert (CloudWatch etc.)
            log.error("CRITICAL: Failed to push event id={} to SQS: {}",
                    event.getId(), e.getMessage(), e);
        }
    }
}
```

---

## Full Retry Timeline

```
T+0 min    Attempt 1  →  REST API call  →  ❌ 500 error
                         message written to [main-topic-retry-0]

T+15 min   Attempt 2  →  REST API call  →  ❌ 500 error
                         message written to [main-topic-retry-0] again

T+30 min   Attempt 3  →  REST API call  →  ❌ 500 error

T+45 min   Attempt 4  →  REST API call  →  ❌ 500 error

T+60 min   Attempt 5  →  REST API call  →  ❌ 500 error
                         @DltHandler fires
                         SqsClient.sendMessage() → SQS Queue

T+60 min   SQS        →  Alert triggered → team investigates
```

---

## Partition Strategy for EKS Auto-Scaling

```
# Rule: partition count should equal HPA maxReplicas
# So every pod can be active even at maximum scale

kafka-topics --create \
  --bootstrap-server $KAFKA_BROKERS \
  --topic main-topic \
  --partitions 12 \             ← matches HPA maxReplicas: 12
  --replication-factor 3        ← MSK default (survives 1 broker failure)

# HPA config (values.yaml or deployment manifest)
autoscaling:
  minReplicas: 2
  maxReplicas: 12               ← matches topic partitions

# Result at different pod counts:
#   2 pods  →  each handles 6 partitions   (works fine)
#   6 pods  →  each handles 2 partitions   (works fine)
#  12 pods  →  each handles 1 partition    (optimal throughput)
#  14 pods  →  2 pods idle                 (harmless, Kafka limitation)
```

---

## Handling 4xx vs 5xx Differently

Not all failures should retry. A **bad payload (400)** will never succeed — retrying wastes time.

```java
@KafkaListener(topics = "${app.kafka.topic}", groupId = "...")
public void consume(YourEvent event) {
    try {
        downstreamApiClient.send(event);
    } catch (HttpClientErrorException e) {
        // 4xx — bad data, will never succeed, skip retries → go straight to SQS
        log.warn("Non-retryable error for event id={}: {}", event.getId(), e.getMessage());
        pushToSqs(event, "NON_RETRYABLE: " + e.getMessage());
        // do NOT rethrow — offset will be committed, no retry
    }
    // 5xx / timeout → let it propagate → @RetryableTopic picks it up
}
```

---

## Moving to MSK — Only Config Changes Needed

```yaml
# Change only these in application.yml — zero Java code changes

spring:
  kafka:
    bootstrap-servers: b-1.your-cluster.kafka.eu-west-1.amazonaws.com:9098
    consumer:
      properties:
        security.protocol: SASL_SSL
        sasl.mechanism: AWS_MSK_IAM
        sasl.jaas.config: software.amazon.msk.auth.iam.IAMLoginModule required;
        sasl.client.callback.handler.class: >
          software.amazon.msk.auth.iam.IAMClientCallbackHandler
```

Add MSK IAM auth dependency:
```xml
<dependency>
    <groupId>software.amazon.msk</groupId>
    <artifactId>aws-msk-iam-auth</artifactId>
    <version>2.1.1</version>
</dependency>
```

---

## Summary

| Concern | Solution |
|---------|---------|
| Consume from external Kafka | `@KafkaListener` with external `bootstrap-servers` |
| Retry 15 min interval | `@RetryableTopic(backoff = @Backoff(delay = 900_000))` |
| Max 5 attempts | `attempts = "5"` |
| One pod per message (EKS) | Kafka consumer group — guaranteed by design |
| Pod auto-scaling | Set partitions = HPA `maxReplicas` |
| Pod dies mid-retry | Retry message stays in Kafka, rebalance covers it |
| After 5 failures → SQS | `@DltHandler` + `SqsClient.sendMessage()` |
| 4xx vs 5xx handling | Catch `HttpClientErrorException` separately, skip retry |
| Moving to MSK | Only `application.yml` changes — no Java code changes |
