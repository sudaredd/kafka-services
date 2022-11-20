package com.example.consumer.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.event.ListenerContainerIdleEvent;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.support.TopicPartitionOffset;
import org.springframework.stereotype.Component;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.IntStream;

@Configuration
public class AppDynamicConsumersConfig {

    //https://stackoverflow.com/questions/61950229/dynamically-start-and-off-kafkalistener-just-to-load-previous-messages-at-the-st/61957619#61957619
    private static final Logger logger = LoggerFactory.getLogger(AppDynamicConsumersConfig.class);

    @Bean
    public ApplicationRunner runner(DynamicListener listener, KafkaTemplate<String, String> template,
                                    @Value("${TOPICS}") String topicsInput) {
        return args -> {
            String topics[] = topicsInput.split(",");
            for (String topic : topics) {
                logger.info("Hit enter to start a listener for topic {}", topic);
                System.in.read();
                listener.newContainer(topic);
            }
        };
    }

    @Component
    class DynamicListener {

        private static final Logger LOG = LoggerFactory.getLogger(DynamicListener.class);

        private final ConcurrentKafkaListenerContainerFactory<String, String> factory;

        private final ConcurrentMap<String, AbstractMessageListenerContainer<String, String>> containers
            = new ConcurrentHashMap<>();

        DynamicListener(ConcurrentKafkaListenerContainerFactory<String, String> factory) {
            this.factory = factory;
        }

        void newContainer(String topic) {
            ConcurrentMessageListenerContainer<String, String> container =
                this.factory.createContainer(topic);
            startContainer(container);
        }

        private void startContainer(ConcurrentMessageListenerContainer<String, String> container) {
            String groupId = UUID.randomUUID().toString();
            container.getContainerProperties().setGroupId(groupId);
            container.setupMessageListener((MessageListener) record -> {
                LOG.info("record {}", record);
            });
            this.containers.put(groupId, container);
            container.start();
        }

        void newContainer(String topic, int partition) {
            ConcurrentMessageListenerContainer<String, String> container =
                this.factory.createContainer(new TopicPartitionOffset(topic, partition));
            startContainer(container);
        }

        @EventListener
        public void idle(ListenerContainerIdleEvent event) {
            AbstractMessageListenerContainer<String, String> container = this.containers.remove(
                event.getContainer(ConcurrentMessageListenerContainer.class).getContainerProperties().getGroupId());
            if (container != null) {
                LOG.info("Stopping idle container");
                container.stop(() -> LOG.info("Stopped"));
            }
        }

    }
}
