package com.eventstreaming.producer.controller;

import com.eventstreaming.producer.model.ChatMessage;
import com.eventstreaming.producer.service.KafkaProducerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/messages")
@CrossOrigin(origins = "*")
@RequiredArgsConstructor
public class MessageController {

    private final KafkaProducerService producerService;

    /**
     * POST /api/messages
     * Body: { "sender": "Alice", "content": "Hello world!", "room": "general" }
     * Publishes the message to Kafka and returns the enriched message with id + timestamp.
     */
    @PostMapping
    public ResponseEntity<ChatMessage> sendMessage(@RequestBody SendMessageRequest request) {
        log.info("REST request to send message from sender='{}'", request.sender());

        ChatMessage message = ChatMessage.of(
                request.sender(),
                request.content(),
                request.room()
        );

        ChatMessage sent = producerService.sendMessage(message);
        return ResponseEntity.ok(sent);
    }

    /**
     * GET /api/messages/history
     * Returns all messages sent in this producer session.
     */
    @GetMapping("/history")
    public ResponseEntity<List<ChatMessage>> getHistory() {
        return ResponseEntity.ok(producerService.getSentMessages());
    }

    /**
     * GET /api/health
     * Simple health check endpoint for the UI status badge.
     */
    @GetMapping("/health")
    public ResponseEntity<Map<String, String>> health() {
        return ResponseEntity.ok(Map.of(
                "status", "UP",
                "service", "kafka-producer",
                "topic", "chat-events"
        ));
    }

    // ── Inner request DTO ────────────────────────────────────────────────────

    public record SendMessageRequest(String sender, String content, String room) {}
}
