package me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter.outbound.persistence.postgres;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import me.neobliz1.ecomonitoring.platform.history.domain.inbound.HistoricalService;
import me.neobliz1.ecomonitoring.platform.history.domain.model.entity.WeatherMapBucket;
import me.neobliz1.ecomonitoring.platform.history.domain.outbound.HistoricalPersistenceRepository;
import me.neobliz1.ecomonitoring.platform.history.domain.outbound.HistoricalQueryRepository;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.WeatherMap;
import org.jspecify.annotations.NonNull;
import org.springframework.orm.ObjectOptimisticLockingFailureException;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Slf4j
@RequiredArgsConstructor
public class TelemetryHistoryPersistenceRepositoryAdapter implements HistoricalPersistenceRepository {

    private final HistoricalQueryRepository queryRepositoryAdapter;
    private final HistoricalService weatherMapConverter;
    private final HistoricalQueryJpaRepository jpaRepository;

    @Override
    @Transactional(isolation = Isolation.READ_COMMITTED)
    public void persistTelemetryRecord(WeatherMap weatherMap) {
        if(log.isDebugEnabled()) {
            log.debug("Processing persistence loop for WeatherMap timestamp bucket: {}", weatherMap.getTimestampBucket());
        }
        try {
            WeatherMapBucket bucket = getOrCreateWeatherBucket(weatherMap);
            weatherMapConverter.extractTelemetryFromWeatherMap(weatherMap, bucket);
            jpaRepository.save(allEntityFlusher(bucket));
            if(log.isDebugEnabled()) {
                log.debug("✅ Successfully persisted WeatherMap snapshot bucket: {}", bucket.getTimestampBucket());
            }
        } catch(ObjectOptimisticLockingFailureException e) {
            log.warn("⚠️ Optimistic Lock Conflict caught for bucket: {}. Transaction will be rolled back safely.",
                    weatherMap.getTimestampBucket());
            throw e;
        }
    }

    private @NonNull WeatherMapBucket getOrCreateWeatherBucket(WeatherMap weatherMap) {
        return queryRepositoryAdapter
                .findByTimestampBucketAndIntervalMinutes(weatherMap.getTimestampBucket(), weatherMap.getIntervalMinutes())
                .orElseGet(() -> new WeatherMapBucket(
                        UUID.randomUUID(),
                        weatherMap.getTimestampBucket(),
                        weatherMap.getIntervalMinutes()
                ));
    }

    private WeatherMapBucket allEntityFlusher(WeatherMapBucket bucket) {
        return jpaRepository.saveAndFlush(bucket);
    }
}
