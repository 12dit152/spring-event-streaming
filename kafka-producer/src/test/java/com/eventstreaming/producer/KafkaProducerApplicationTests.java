package com.eventstreaming.producer;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

@SpringBootTest
@DirtiesContext
@EmbeddedKafka(partitions = 1, brokerProperties = { "listeners=PLAINTEXT://localhost:9093", "port=9093" })
class KafkaProducerApplicationTests {

    @Test
    void contextLoads() {
        // Verifies that the Spring context starts successfully with embedded Kafka
    }
}
