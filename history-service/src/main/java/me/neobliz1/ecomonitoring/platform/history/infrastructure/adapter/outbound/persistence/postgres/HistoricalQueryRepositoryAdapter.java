package me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter.outbound.persistence.postgres;

import lombok.RequiredArgsConstructor;
import me.neobliz1.ecomonitoring.platform.history.domain.model.entity.WeatherMapBucket;
import me.neobliz1.ecomonitoring.platform.history.domain.outbound.HistoricalQueryRepository;

import java.util.Optional;
import java.util.UUID;

@RequiredArgsConstructor
public class HistoricalQueryRepositoryAdapter implements HistoricalQueryRepository {

    private final HistoricalQueryJpaRepository jpaRepository;

    @Override
    public WeatherMapBucket upsertBucket(UUID id, Long timestampBucket, Integer intervalMinutes) {
        return jpaRepository.upsertBucket(id, timestampBucket, intervalMinutes);
    }

    @Override
    public Optional<WeatherMapBucket> findByTimestampBucketAndIntervalMinutes(Long timestampBucket, Integer intervalMinutes) {
        return jpaRepository.findByTimestampBucketAndIntervalMinutes(timestampBucket, intervalMinutes);
    }
}
