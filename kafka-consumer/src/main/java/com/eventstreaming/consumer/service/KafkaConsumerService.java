package com.eventstreaming.consumer.service;

import com.eventstreaming.consumer.model.ChatMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

@Slf4j
@Service
public class KafkaConsumerService {

    /** All messages received since startup (in-memory). */
    private final List<ChatMessage> receivedMessages = Collections.synchronizedList(new ArrayList<>());

    /**
     * Active SSE emitters — one per connected browser tab.
     * CopyOnWriteArrayList is safe for concurrent add/remove during iteration.
     */
    private final List<SseEmitter> emitters = new CopyOnWriteArrayList<>();

    // ── Kafka Listener ───────────────────────────────────────────────────────

    /**
     * Listens to the 'chat-events' topic and fans the message out to:
     *  1. The in-memory store (for history)
     *  2. All connected SSE clients (for real-time push to browser)
     */
    @KafkaListener(
            topics = "${app.kafka.topic}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(ChatMessage message) {
        log.info("Received message from Kafka | sender='{}' content='{}'",
                message.getSender(), message.getContent());

        receivedMessages.add(message);
        broadcastToSseClients(message);
    }

    // ── SSE Management ───────────────────────────────────────────────────────

    /**
     * Registers a new SSE emitter and immediately replays all existing messages
     * so a freshly-opened browser tab sees the full history.
     */
    public SseEmitter registerEmitter() {
        SseEmitter emitter = new SseEmitter(Long.MAX_VALUE);

        emitter.onCompletion(() -> emitters.remove(emitter));
        emitter.onTimeout(() -> emitters.remove(emitter));
        emitter.onError(e -> emitters.remove(emitter));

        emitters.add(emitter);

        // Replay existing messages to the newly connected client
        List<ChatMessage> snapshot = new ArrayList<>(receivedMessages);
        for (ChatMessage msg : snapshot) {
            try {
                emitter.send(SseEmitter.event()
                        .name("message")
                        .data(msg));
            } catch (IOException e) {
                log.warn("Failed to replay message to new SSE client: {}", e.getMessage());
                emitters.remove(emitter);
                return emitter;
            }
        }

        log.debug("New SSE client registered. Total active emitters: {}", emitters.size());
        return emitter;
    }

    /**
     * Pushes a new message to all active SSE connections.
     * Removes any emitter that fails (broken connection).
     */
    private void broadcastToSseClients(ChatMessage message) {
        List<SseEmitter> dead = new ArrayList<>();

        for (SseEmitter emitter : emitters) {
            try {
                emitter.send(SseEmitter.event()
                        .name("message")
                        .data(message));
            } catch (IOException e) {
                log.warn("SSE client disconnected, removing emitter: {}", e.getMessage());
                dead.add(emitter);
            }
        }

        emitters.removeAll(dead);
    }

    // ── Query ────────────────────────────────────────────────────────────────

    /**
     * Returns an unmodifiable snapshot of all received messages.
     */
    public List<ChatMessage> getReceivedMessages() {
        return Collections.unmodifiableList(new ArrayList<>(receivedMessages));
    }

    /**
     * Returns count of currently connected SSE clients.
     */
    public int getActiveConnections() {
        return emitters.size();
    }
}
