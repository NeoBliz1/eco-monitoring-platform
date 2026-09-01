package me.neobliz1.ecomonitoring.platform.history.domain.port.outbound;

import me.neobliz1.ecomonitoring.platform.history.domain.model.entity.WeatherGridCellMetric;
import me.neobliz1.ecomonitoring.platform.history.domain.model.entity.WeatherMapBucket;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface HistoricalQueryRepository {

    Optional<WeatherMapBucket> findByTimestampBucketAndIntervalMinutes(Long timestampBucket, Integer intervalMinutes);

    List<WeatherGridCellMetric> findByBucketIdAndSpatialBox(UUID bucketId, double minLat, double maxLat, double minLon, double maxLon);
}