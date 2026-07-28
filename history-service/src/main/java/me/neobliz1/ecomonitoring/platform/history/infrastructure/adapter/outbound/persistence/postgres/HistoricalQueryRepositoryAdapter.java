package me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter.outbound.persistence.postgres;

import lombok.RequiredArgsConstructor;
import me.neobliz1.ecomonitoring.platform.history.domain.model.entity.WeatherMapBucket;
import me.neobliz1.ecomonitoring.platform.history.domain.outbound.HistoricalQueryRepository;

import java.util.Optional;

@RequiredArgsConstructor
public class HistoricalQueryRepositoryAdapter implements HistoricalQueryRepository {

    private final HistoricalQueryJpaRepository jpaRepository;

    @Override
    public Optional<WeatherMapBucket> findByTimestampBucketAndIntervalMinutes(Long timestampBucket, Integer intervalMinutes) {
        return jpaRepository.findByTimestampBucketAndIntervalMinutes(timestampBucket, intervalMinutes);
    }
}
