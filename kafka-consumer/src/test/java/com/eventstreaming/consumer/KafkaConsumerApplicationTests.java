package com.eventstreaming.consumer;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

@SpringBootTest
@DirtiesContext
@EmbeddedKafka(partitions = 1)
class KafkaConsumerApplicationTests {

    @Test
    void contextLoads() {
        // Verifies that the Spring context starts successfully with embedded Kafka
    }
}
