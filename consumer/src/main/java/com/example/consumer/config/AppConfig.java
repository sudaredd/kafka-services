package com.example.consumer.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.support.TopicPartitionOffset;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.springframework.kafka.support.TopicPartitionOffset.SeekPosition.END;

@Configuration
public class AppConfig {

    @Bean
    ApplicationRunner setPartiton(KafkaListenerEndpointRegistry registry,
                                  ConcurrentKafkaListenerContainerFactory<String, String> factory,
                                  Environment environment, KafkaProperties kafkaProperties) {
        return args -> {
//            kafkaProperties.setBootstrapServers(List.of("localhost:9090"));
            String topic = environment.getProperty("TOPIC");
            if (topic != null) {
                factory.getConsumerFactory().updateConfigs(Map.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                    "localhost:9092"));
                factory.setBatchListener(true);
                int partition = Integer.parseInt(Objects.requireNonNull(environment.getProperty("PARTITION")));
                ConcurrentMessageListenerContainer<String, String> container1 =
                    factory.createContainer(new TopicPartitionOffset(topic, partition, END));
                container1.getContainerProperties().setMessageListener(
                    registry.getListenerContainer("byPartitionId").getContainerProperties().getMessageListener());
                container1.getContainerProperties().setGroupId("topic1-0-group2");
                container1.start();
            }
        };
    }


}
