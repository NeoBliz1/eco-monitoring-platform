package me.neobliz1.ecomonitoring.platform.history.infrastructure.config;

import static java.util.Objects.nonNull;
import static me.neobliz1.ecomonitoring.platform.common.constant.PlatformConstants.SCHEMA_REGISTRY_URL;

import jakarta.validation.ConstraintViolationException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import me.neobliz1.ecomonitoring.platform.history.domain.port.outbound.HistoricalPersistenceRepository;
import me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter.inbound.kafka.HistoricalTelemetryDlqListener;
import me.neobliz1.ecomonitoring.platform.model.exception.L1CacheNotAvailableException;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.WeatherMap;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.hibernate.exception.JDBCConnectionException;
import org.jspecify.annotations.NonNull;
import org.springframework.context.MessageSourceResolvable;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
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
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.util.backoff.FixedBackOff;
import org.springframework.validation.method.MethodValidationException;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class KafkaListenerConfig {

    private final HistoryInfrastructureProperties infraProps;

    @Bean
    public ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(ConsumerFactory<Object, Object> consumerFactory,
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
                DataAccessResourceFailureException.class,
                JDBCConnectionException.class
        );
        errorHandler.addNotRetryableExceptions(
                ListenerExecutionFailedException.class,
                MethodValidationException.class,
                jakarta.validation.ConstraintViolationException.class,
                jakarta.validation.ValidationException.class,
                NullPointerException.class,
                IllegalArgumentException.class,
                DataIntegrityViolationException.class,
                ConstraintViolationException.class
        );
        setRetryListeners(errorHandler);
        return errorHandler;
    }

    private @NonNull DefaultErrorHandler getErrorHandler(KafkaTemplate<String, WeatherMap> dlqKafkaTemplate) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(dlqKafkaTemplate,
                (record, exception) -> {
                    log.error("Routing record from topic {} to DLQ due to fatal failure. Exception: {}",
                            record.topic(), exception.getMessage());
                    return new TopicPartition(record.topic()+".DLT", record.partition());
                });
        setCustomExDlqHeadersCreator(recoverer);
        FixedBackOff backOff = new FixedBackOff(
                Duration.ofSeconds(infraProps.getKafka().getConsumer().getBackoff().getInterval()).toMillis(),
                infraProps.getKafka().getConsumer().getBackoff().getMaxAttempts()
        );
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, backOff);
        errorHandler.setAckAfterHandle(true);
        return errorHandler;
    }

    @Bean
    public HistoricalTelemetryDlqListener historicalTelemetryDlqListener(HistoricalPersistenceRepository historicalPersistenceRepository) {
        return new HistoricalTelemetryDlqListener(historicalPersistenceRepository);
    }

    private void setRetryListeners(DefaultErrorHandler errorHandler) {
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
    }

    private void setCustomExDlqHeadersCreator(DeadLetterPublishingRecoverer recoverer) {
        recoverer.setExceptionHeadersCreator((kafkaHeaders, exception, isKey, headerNames) -> {
            Optional<MethodValidationException> validationException = recursiveGetMethodValidationException(exception);
            String exceptionMessageValue = validationException.map(e -> e.getAllErrors().stream()
                    .map(MessageSourceResolvable::getDefaultMessage)
                    .collect(java.util.stream.Collectors.joining("; "))).orElseGet(exception::getMessage);
            kafkaHeaders.add(new RecordHeader(
                    headerNames.getExceptionInfo().getExceptionMessage(),
                    exceptionMessageValue.getBytes(StandardCharsets.UTF_8)
            ));
            if(exception.getCause()!=null) {
                StringWriter sw = new StringWriter();
                exception.printStackTrace(new PrintWriter(sw));
                kafkaHeaders.add(new RecordHeader(
                        headerNames.getExceptionInfo().getExceptionStacktrace(),
                        sw.toString().getBytes(StandardCharsets.UTF_8)
                ));
            }
        });
    }

    private Optional<MethodValidationException> recursiveGetMethodValidationException(@NonNull Throwable currentCause) {
        Optional<MethodValidationException> validationEx = Optional.empty();
        while(currentCause!=null) {
            if(currentCause instanceof MethodValidationException ex) {
                validationEx = Optional.of(ex);
                break;
            }
            currentCause = currentCause.getCause();
        }
        return validationEx;
    }
}