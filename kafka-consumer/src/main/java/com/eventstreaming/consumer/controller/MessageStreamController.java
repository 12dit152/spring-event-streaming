package com.eventstreaming.consumer.controller;

import com.eventstreaming.consumer.model.ChatMessage;
import com.eventstreaming.consumer.service.KafkaConsumerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.List;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/messages")
@CrossOrigin(origins = "*")
@RequiredArgsConstructor
public class MessageStreamController {

    private final KafkaConsumerService consumerService;

    /**
     * GET /api/messages/stream
     * Server-Sent Events endpoint. The browser subscribes once and receives
     * every new Kafka message pushed in real-time — no polling needed.
     */
    @GetMapping(value = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter stream() {
        log.info("New SSE client connected");
        return consumerService.registerEmitter();
    }

    /**
     * GET /api/messages
     * Returns all received messages as a JSON array (REST fallback / history).
     */
    @GetMapping
    public ResponseEntity<List<ChatMessage>> getMessages() {
        return ResponseEntity.ok(consumerService.getReceivedMessages());
    }

    /**
     * GET /api/health
     * Health check — also returns how many SSE clients are connected.
     */
    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> health() {
        return ResponseEntity.ok(Map.of(
                "status", "UP",
                "service", "kafka-consumer",
                "topic", "chat-events",
                "activeConnections", consumerService.getActiveConnections(),
                "totalReceived", consumerService.getReceivedMessages().size()
        ));
    }
}
