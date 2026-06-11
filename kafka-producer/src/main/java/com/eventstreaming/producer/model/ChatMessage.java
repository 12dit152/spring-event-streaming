package com.eventstreaming.producer.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ChatMessage {

    private String id;
    private String sender;
    private String content;
    private String room;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime timestamp;

    /**
     * Factory method to create a new outbound message with generated id and current timestamp.
     */
    public static ChatMessage of(String sender, String content, String room) {
        return ChatMessage.builder()
                .id(UUID.randomUUID().toString())
                .sender(sender)
                .content(content)
                .room(room != null ? room : "general")
                .timestamp(LocalDateTime.now())
                .build();
    }
}
