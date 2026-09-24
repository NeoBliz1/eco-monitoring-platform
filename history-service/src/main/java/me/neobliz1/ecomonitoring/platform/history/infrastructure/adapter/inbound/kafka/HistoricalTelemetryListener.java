package me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter.inbound.kafka;

import static me.neobliz1.ecomonitoring.platform.common.constant.PlatformConstants.HISTORICAL_WEATHER_MAP_KAFKA_LISTENER_SPAN;
import static me.neobliz1.ecomonitoring.platform.common.util.PlatformCommonUtils.addLinksToConsumerSpan;
import static me.neobliz1.ecomonitoring.platform.common.util.PlatformCommonUtils.getHeadersTextMapGetter;
import static me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter.outbound.persistence.postgres.HistoricalPersistenceRepositoryAdapter.getBucketId;

import com.google.common.util.concurrent.Striped;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import me.neobliz1.ecomonitoring.platform.history.domain.port.outbound.HistoricalPersistenceRepository;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.WeatherMap;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;

import java.util.UUID;
import java.util.concurrent.locks.Lock;

@Slf4j
@RequiredArgsConstructor
public class HistoricalTelemetryListener {

    private final HistoricalPersistenceRepository historicalPersistenceRepository;
    private final Tracer tracer = GlobalOpenTelemetry.getTracer("weather-history-consumer");
    private final Striped<Lock> locker = Striped.lock(2048);

    @SuppressWarnings("unused")
    @KafkaListener(
            topics = "${spring.kafka.topic.weather-history}",
            groupId = "${spring.kafka.consumer.group-id}"
    )
    public void consumeHistoricalWeatherMap(ConsumerRecord<String, WeatherMap> record) {
        WeatherMap weatherMap = record.value();
        if(weatherMap==null) {
            log.warn("Received null WeatherMap payload from partition {} at offset {}. Skipping corrupt record.",
                    record.partition(), record.offset());
            return;
        }

        SpanBuilder consumerSpanBuilder = getConsumerSpan(weatherMap);
        addLinksToConsumerSpan(weatherMap, consumerSpanBuilder);
        Span consumerSpan = consumerSpanBuilder.startSpan();
        try(Scope ignored = consumerSpan.makeCurrent()) {
            if(log.isDebugEnabled()) {
                log.debug("Received aggregated WeatherMap stream chunk from Kafka. Bucket: [{}], Cells size: [{}]",
                        weatherMap.getTimestampBucket(), weatherMap.getGridCellsCount());
            }
            UUID bucketId = getBucketId(weatherMap);
            Lock lock = locker.get(bucketId);
            lock.lock();
            try {
                historicalPersistenceRepository.persistTelemetryRecord(weatherMap);
            } finally {
                lock.unlock();
            }
            if(log.isDebugEnabled()) {
                log.debug("Offset committed to Kafka broker for bucket: {}", weatherMap.getTimestampBucket());
            }
        } catch(Exception e) {
            consumerSpan.recordException(e);
            consumerSpan.setStatus(StatusCode.ERROR, e.getMessage());
            log.error("Failed to store WeatherMap bucket [{}]. Offset will NOT be acknowledged. Error: {}",
                    weatherMap.getTimestampBucket(), e.getMessage(), e);
            throw e;
        } finally {
            consumerSpan.end();
        }
    }

    private SpanBuilder getConsumerSpan(WeatherMap weatherMap) {
        Context parentContext = GlobalOpenTelemetry.getPropagators().getTextMapPropagator()
                .extract(Context.current(), weatherMap, getHeadersTextMapGetter());
        return tracer.spanBuilder(HISTORICAL_WEATHER_MAP_KAFKA_LISTENER_SPAN)
                .setParent(parentContext)
                .setAttribute("weather.bucket.time", weatherMap.getTimestampBucket());
    }
}
