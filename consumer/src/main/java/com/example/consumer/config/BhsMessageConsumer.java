package com.example.consumer.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class BhsMessageConsumer {

    @KafkaListener(id = "byPartitionId", topics = "${kafka.topic:dummy}", autoStartup = "false")
    void listen(@Payload String payload,  @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
                @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                @Header(KafkaHeaders.RECEIVED_TIMESTAMP) long ts) {
        log.info(payload);
        log.info("headers partition {}, topic {}, timestamp {}", partition, topic, ts);
    }

    @KafkaListener(id = "byConsumer", topics = "${TOPIC:transactions}", groupId = "byConsumer")
    void listenMsg(@Payload String payload,  @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
                @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                @Header(KafkaHeaders.RECEIVED_TIMESTAMP) long ts) {
        log.info(payload);
        log.info("headers partition {}, topic {}, timestamp {}", partition, topic, ts);
    }
}
