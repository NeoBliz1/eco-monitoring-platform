package me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter.outbound.persistence.postgres;

import lombok.RequiredArgsConstructor;
import me.neobliz1.ecomonitoring.platform.history.domain.model.entity.WeatherGridCellMetric;
import me.neobliz1.ecomonitoring.platform.history.domain.model.entity.WeatherMapBucket;
import me.neobliz1.ecomonitoring.platform.history.domain.port.outbound.HistoricalQueryRepository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@RequiredArgsConstructor
public class HistoricalQueryRepositoryAdapter implements HistoricalQueryRepository {

    private final HistoricalWeatherMapJpaRepository weatherMapJpaRepository;
    private final HistoricalWeatherGridCellJpaRepository weatherGridCellJpaRepository;

    @Override
    public Optional<WeatherMapBucket> findByTimestampBucketAndIntervalMinutes(Long timestampBucket, Integer intervalMinutes) {
        return weatherMapJpaRepository.findByTimestampBucketAndIntervalMinutes(timestampBucket, intervalMinutes);
    }

    @Override
    public List<WeatherGridCellMetric> findByBucketIdAndSpatialBox(UUID bucketId, double minLat, double maxLat, double minLon, double maxLon) {
        return weatherGridCellJpaRepository.findByBucketIdAndSpatialBox(bucketId, minLat, maxLat, minLon, maxLon);
    }
}
