package me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter.outbound.persistence.postgres;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import me.neobliz1.ecomonitoring.platform.history.domain.model.entity.WeatherMapBucket;
import me.neobliz1.ecomonitoring.platform.history.domain.port.inbound.HistoricalDataConvertService;
import me.neobliz1.ecomonitoring.platform.history.domain.port.outbound.HistoricalPersistenceRepository;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.WeatherMap;
import org.jspecify.annotations.Nullable;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Slf4j
@RequiredArgsConstructor
public class HistoricalPersistenceRepositoryAdapter implements HistoricalPersistenceRepository {

    private final HistoricalDataConvertService weatherMapConverter;
    private final HistoricalWeatherMapJpaRepository jpaRepository;
    @Nullable
    private final HistoricalTxIdRepositoryAdapter txIdAdapter;

    @Override
    @Transactional(isolation = Isolation.READ_COMMITTED)
    public void persistTelemetryRecord(@NonNull WeatherMap weatherMap) {
        if(log.isDebugEnabled()) {
            log.debug("Processing persistence loop for WeatherMap timestamp bucket: {}", weatherMap.getTimestampBucket());
        }
        WeatherMapBucket bucket = upsertBucket(UUID.randomUUID(),
                weatherMap.getTimestampBucket(),
                weatherMap.getIntervalMinutes());
        weatherMapConverter.extractTelemetryFromWeatherMap(weatherMap, bucket);
        jpaRepository.saveAndFlush(bucket);
        if(txIdAdapter!=null) {
            if(log.isDebugEnabled()) {
                log.debug("Confirmation profile active. Executing transaction batch log tracking.");
            }
            txIdAdapter.processTxIdsHistoryBatch(weatherMap);
        }
        if(log.isDebugEnabled()) {
            log.debug("Successfully persisted WeatherMap snapshot bucket: {}", bucket.getTimestampBucket());
        }
    }

    @Override
    public WeatherMapBucket upsertBucket(UUID id, Long timestampBucket, Integer intervalMinutes) {
        return jpaRepository.upsertBucket(id, timestampBucket, intervalMinutes);
    }
}
