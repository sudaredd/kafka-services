package com.example.consumer.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.event.ListenerContainerIdleEvent;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.stereotype.Component;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Configuration
public class AppDynamicConsumersConfig {

    //https://stackoverflow.com/questions/61950229/dynamically-start-and-off-kafkalistener-just-to-load-previous-messages-at-the-st/61957619#61957619
    private static final Logger logger = LoggerFactory.getLogger(AppDynamicConsumersConfig.class);

    @Bean
    public ApplicationRunner runner(DynamicListener listener, KafkaTemplate<String, String> template,
                                    @Value("${TOPICS}") String topicsInput, KafkaListenerEndpointRegistry registry) {
        return args -> {
            String topics[] = topicsInput.split(",");
            for (String topic : topics) {
                listener.newContainer(topic, registry);
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

        void newContainer(String topic, KafkaListenerEndpointRegistry registry) {
            ConcurrentMessageListenerContainer<String, String> container =
                this.factory.createContainer(topic);
            startContainer(container, registry, topic);
        }

        private void startContainer(ConcurrentMessageListenerContainer<String, String> container,
                                    KafkaListenerEndpointRegistry registry, String topic) {
            String groupId = UUID.randomUUID().toString();
            container.getContainerProperties().setGroupId(groupId);
           container.setBeanName(topic);

/*            container.setupMessageListener((MessageListener) (record) -> {
                LOG.info("record {} headers {}", record.value(), record.headers());
            });*/

            container.getContainerProperties().setMessageListener(
                registry.getListenerContainer("byConsumer").getContainerProperties().getMessageListener());

            this.containers.put(groupId, container);
            container.start();
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
