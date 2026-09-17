package me.neobliz1.ecomonitoring.platform.history.infrastructure.config;

import static java.util.Objects.nonNull;
import static me.neobliz1.ecomonitoring.platform.common.constant.PlatformConstants.SCHEMA_REGISTRY_URL;

import jakarta.validation.ConstraintViolationException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import me.neobliz1.ecomonitoring.platform.model.exception.L1CacheNotAvailableException;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.WeatherMap;
import org.apache.kafka.common.TopicPartition;
import org.hibernate.exception.JDBCConnectionException;
import org.hibernate.exception.LockAcquisitionException;
import org.jspecify.annotations.NonNull;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.CannotAcquireLockException;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.PessimisticLockingFailureException;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class KafkaListenerConfig {

    private final HistoryInfrastructureProperties infraProps;

    @Bean
    public ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
            ConsumerFactory<Object, Object> consumerFactory,
            KafkaTemplate<String, WeatherMap> dlqKafkaTemplate) {
        Map<String, Object> consumerConfig = new HashMap<>(consumerFactory.getConfigurationProperties());
        String schemaRegistryUrl = infraProps.getKafka().getStreams().getProperties().getSchema().getRegistry().getUrl();
        consumerConfig.put(SCHEMA_REGISTRY_URL, schemaRegistryUrl);
        ConsumerFactory<Object, Object> updatingConsumerFactory = new DefaultKafkaConsumerFactory<>(consumerConfig);
        if(dlqKafkaTemplate.getProducerFactory() instanceof DefaultKafkaProducerFactory<String, WeatherMap> producerFactory) {
            producerFactory.updateConfigs(Map.of(SCHEMA_REGISTRY_URL, schemaRegistryUrl));
        }
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(updatingConsumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        DefaultErrorHandler errorHandler = getDefaultErrorHandler(dlqKafkaTemplate);
        factory.setCommonErrorHandler(errorHandler);
        return factory;
    }

    private @NonNull DefaultErrorHandler getDefaultErrorHandler(KafkaTemplate<String, WeatherMap> dlqKafkaTemplate) {
        DefaultErrorHandler errorHandler = getErrorHandler(dlqKafkaTemplate);
        errorHandler.addRetryableExceptions(
                L1CacheNotAvailableException.class,
                PessimisticLockingFailureException.class,
                CannotAcquireLockException.class,
                LockAcquisitionException.class,
                DataAccessResourceFailureException.class,
                JDBCConnectionException.class
        );
        errorHandler.addNotRetryableExceptions(
                NullPointerException.class,
                IllegalArgumentException.class,
                DataIntegrityViolationException.class,
                ConstraintViolationException.class
        );
        errorHandler.setRetryListeners((record, exception, deliveryAttempt) -> {
            log.error("ING_CRASH [Attempt {}] - Topic: {} Partition: {} Offset: {}",
                    deliveryAttempt, record.topic(), record.partition(), record.offset());
            if(nonNull(exception)) {
                log.error("Exception: {}", exception.getLocalizedMessage());
                if(nonNull(exception.getCause())) {
                    log.error("Inner Exception Cause: {}", exception.getCause().getLocalizedMessage());
                }
            }
        });
        return errorHandler;
    }

    private @NonNull DefaultErrorHandler getErrorHandler(KafkaTemplate<String, WeatherMap> dlqKafkaTemplate) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(dlqKafkaTemplate,
                (record, exception) -> {
                    log.error("Routing record from topic {} to DLQ due to fatal failure. Exception: {}",
                            record.topic(), exception.getMessage());
                    return new TopicPartition(record.topic()+".DLT", record.partition());
                });
        FixedBackOff backOff = new FixedBackOff(
                Duration.ofSeconds(infraProps.getKafka().getConsumer().getBackoff().getInterval()).toMillis(),
                infraProps.getKafka().getConsumer().getBackoff().getMaxAttempts()
        );
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, backOff);
        errorHandler.setAckAfterHandle(true);
        return errorHandler;
    }
}