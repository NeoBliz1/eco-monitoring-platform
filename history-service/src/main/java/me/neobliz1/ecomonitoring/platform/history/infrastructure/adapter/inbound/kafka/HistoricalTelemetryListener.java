package me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter.inbound.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import me.neobliz1.ecomonitoring.platform.history.domain.port.outbound.HistoricalPersistenceRepository;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.WeatherMap;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;

@Slf4j
@RequiredArgsConstructor
public class HistoricalTelemetryListener {

    private final HistoricalPersistenceRepository historicalPersistenceRepository;

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
        if(log.isDebugEnabled()) {
            log.debug("Received aggregated WeatherMap stream chunk from Kafka. Bucket: [{}], Cells size: [{}]",
                    weatherMap.getTimestampBucket(), weatherMap.getGridCellsCount());
        }
        historicalPersistenceRepository.persistTelemetryRecord(weatherMap);
        if(log.isDebugEnabled()) {
            log.debug("Offset committed to Kafka broker for bucket: {}", weatherMap.getTimestampBucket());
        }
    }
}
