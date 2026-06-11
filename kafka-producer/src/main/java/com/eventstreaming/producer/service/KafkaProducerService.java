package com.eventstreaming.producer.service;

import com.eventstreaming.producer.model.ChatMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaProducerService {

    private final KafkaTemplate<String, ChatMessage> kafkaTemplate;

    @Value("${app.kafka.topic}")
    private String topic;

    /** In-memory store of sent messages for history endpoint */
    private final List<ChatMessage> sentMessages = Collections.synchronizedList(new ArrayList<>());

    /**
     * Publishes a ChatMessage to the Kafka topic.
     * Uses the message id as the partition key so messages from the same sender
     * are spread evenly rather than always hitting partition 0.
     *
     * @param message the chat message to send
     * @return the same message (with id and timestamp already populated)
     */
    public ChatMessage sendMessage(ChatMessage message) {
        log.info("Publishing message from '{}' to topic '{}': {}", message.getSender(), topic, message.getContent());

        CompletableFuture<SendResult<String, ChatMessage>> future =
                kafkaTemplate.send(topic, message.getId(), message);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Failed to send message id={}: {}", message.getId(), ex.getMessage(), ex);
            } else {
                log.debug("Message id={} sent to partition={} offset={}",
                        message.getId(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });

        sentMessages.add(message);
        return message;
    }

    /**
     * Returns an unmodifiable view of all messages sent in this session.
     */
    public List<ChatMessage> getSentMessages() {
        return Collections.unmodifiableList(sentMessages);
    }
}
