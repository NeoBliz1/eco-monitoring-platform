package me.neobliz1.ecomonitoring.platform.history.domain.outbound;

import me.neobliz1.ecomonitoring.platform.history.domain.model.entity.WeatherMapBucket;

import java.util.Optional;
import java.util.UUID;

public interface HistoricalQueryRepository {

    WeatherMapBucket upsertBucket(UUID id, Long timestampBucket, Integer intervalMinutes);
    Optional<WeatherMapBucket> findByTimestampBucketAndIntervalMinutes(Long timestampBucket, Integer intervalMinutes);
}
