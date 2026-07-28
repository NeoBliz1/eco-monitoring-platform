package me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter.inbound.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import me.neobliz1.ecomonitoring.platform.history.domain.outbound.HistoricalPersistenceRepository;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.WeatherMap;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;

@Slf4j
@RequiredArgsConstructor
public class HistoricalTelemetryListener {

    private final HistoricalPersistenceRepository historicalPersistenceRepository;

    @SuppressWarnings("unused")
    @KafkaListener(
            topics = "${spring.kafka.topic.weather-history}",
            groupId = "${spring.kafka.consumer.group-id}"
    )
    public void consumeHistoricalWeatherMap(ConsumerRecord<String, WeatherMap> record, Acknowledgment ack) {
        WeatherMap weatherMap = record.value();

        if(log.isDebugEnabled()) {
            log.debug("📡 Received aggregated WeatherMap stream chunk from Kafka. Bucket: [{}], Cells size: [{}]",
                    weatherMap.getTimestampBucket(), weatherMap.getGridCellsCount());
        }
        try {
            historicalPersistenceRepository.persistTelemetryRecord(weatherMap);
            ack.acknowledge();
            if(log.isDebugEnabled()) {
                log.debug("✅ Offset committed safely to Kafka broker channel for bucket: {}", weatherMap.getTimestampBucket());
            }
        } catch(Exception e) {
            log.error("❌ Transaction pipeline collapsed while storing WeatherMap bucket [{}]. Offset will NOT be acknowledged! Error: {}",
                    weatherMap.getTimestampBucket(), e.getMessage(), e);
            throw e;
        }
    }
}
