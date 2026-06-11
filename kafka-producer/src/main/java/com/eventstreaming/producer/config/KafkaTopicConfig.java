package com.eventstreaming.producer.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    @Value("${app.kafka.topic}")
    private String topicName;

    /**
     * Auto-creates the Kafka topic on startup if it does not already exist.
     * Uses 3 partitions to allow parallel consumption.
     */
    @Bean
    public NewTopic chatEventsTopic() {
        return TopicBuilder.name(topicName)
                .partitions(3)
                .replicas(1)
                .build();
    }
}
