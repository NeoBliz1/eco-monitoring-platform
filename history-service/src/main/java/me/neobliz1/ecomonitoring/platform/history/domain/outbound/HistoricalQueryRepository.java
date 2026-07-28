package me.neobliz1.ecomonitoring.platform.history.domain.outbound;

import me.neobliz1.ecomonitoring.platform.history.domain.model.entity.WeatherMapBucket;

import java.util.Optional;

public interface HistoricalQueryRepository {

    Optional<WeatherMapBucket> findByTimestampBucketAndIntervalMinutes(Long timestampBucket, Integer intervalMinutes);
}
