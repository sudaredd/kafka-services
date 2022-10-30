package com.example.producer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

@SpringBootApplication
public class ProducerApplication {

    public static void main(String[] args) {
        SpringApplication.run(ProducerApplication.class, args);
    }

    @Component
    static class Processor implements CommandLineRunner {

        private final Producer producer;

        Processor(Producer producer) {
            this.producer = producer;
        }

        @Override
        public void run(String... args) throws Exception {
            for (int i = 0; i < 1000; i++) {
                producer.sendMessage(String.format("Message %s", i), i % 3);
            }
        }
    }

    @Component
    static class Producer {
        @Autowired
        private KafkaTemplate kafkaTemplate;

        @Value("${TOPIC}")
        private String topic;

        public void sendMessage(String payload, int partitionId) {
            Message<String> message = MessageBuilder
                .withPayload(payload)
                .setHeader(KafkaHeaders.PARTITION_ID, partitionId)
                .setHeader(KafkaHeaders.TOPIC, topic)
                .build();
            kafkaTemplate.send(message);
        }

    }
}
