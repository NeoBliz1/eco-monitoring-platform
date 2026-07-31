package me.neobliz1.ecomonitoring.platform.history.infrastructure.config;

import static java.util.Objects.nonNull;
import static me.neobliz1.ecomonitoring.platform.common.constant.PlatformConstants.SCHEMA_REGISTRY_URL;

import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.NonNull;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Configuration
public class KafkaListenerConfig {

    @Value("${spring.kafka.consumer.backoff.interval}")
    private int backoffInterval;
    @Value("${spring.kafka.streams.properties.schema.registry.url}")
    private String schemaRegistryUrl;

    @Bean
    public ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(ConsumerFactory<Object, Object> consumerFactory) {
        Map<String, Object> consumerConfig = new HashMap<>(consumerFactory.getConfigurationProperties());
        consumerConfig.put(SCHEMA_REGISTRY_URL, schemaRegistryUrl);
        ConsumerFactory<Object, Object> updatingConsumerFactory = new DefaultKafkaConsumerFactory<>(consumerConfig);
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(updatingConsumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        DefaultErrorHandler errorHandler = getDefaultErrorHandler();
        factory.setCommonErrorHandler(errorHandler);
        return factory;
    }

    private @NonNull DefaultErrorHandler getDefaultErrorHandler() {
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(new FixedBackOff(Duration.ofSeconds(backoffInterval).toMillis(),
                FixedBackOff.UNLIMITED_ATTEMPTS));
        errorHandler.setRetryListeners((record, exception, deliveryAttempt) -> {
            log.error("🚨 ING_CRASH [Attempt {}] - Partition: {} Offset: {}",
                    deliveryAttempt, record.partition(), record.offset());
            if(nonNull(exception)) {
                log.error("🚨 Exception: {}", exception.getLocalizedMessage());
                if(nonNull(exception.getCause()))
                    log.error("🚨 Inner Exception Cause: {}", exception.getCause().getLocalizedMessage());
            }
        });
        return errorHandler;
    }
}
