package me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter.outbound.persistence.postgres;

import lombok.RequiredArgsConstructor;
import me.neobliz1.ecomonitoring.platform.history.domain.model.entity.WeatherGridCellLayer;
import me.neobliz1.ecomonitoring.platform.history.domain.model.entity.WeatherMapBucket;
import me.neobliz1.ecomonitoring.platform.history.domain.port.outbound.HistoricalQueryRepository;
import me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter.outbound.persistence.postgres.jpa.HistoricalWeatherGridCellJpaRepository;
import me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter.outbound.persistence.postgres.jpa.HistoricalWeatherMapJpaRepository;
import org.jspecify.annotations.NonNull;
import weather.history.SpatialBoxRequest;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@RequiredArgsConstructor
public class HistoricalQueryRepositoryAdapter implements HistoricalQueryRepository {

    private final HistoricalWeatherMapJpaRepository weatherMapJpaRepository;
    private final HistoricalWeatherGridCellJpaRepository weatherGridCellJpaRepository;

    @Override
    public Optional<WeatherMapBucket> findByTimestampBucketAndIntervalMinutes(@NonNull SpatialBoxRequest request) {
        return weatherMapJpaRepository.findByTimestampBucketAndIntervalMinutes(request.getTimestampBucket(), request.getTimeIntervalInMinutes());
    }

    @Override
    public List<WeatherGridCellLayer> findByBucketIdAndSpatialBox(UUID bucketId, @NonNull SpatialBoxRequest request) {
        return weatherGridCellJpaRepository.findByBucketIdAndSpatialBox(bucketId, request.getMinLat(), request.getMaxLat(),
                request.getMinLon(), request.getMaxLon());
    }

    @Override
    public List<WeatherMapBucket> findAllPastBuckets(@NonNull SpatialBoxRequest request) {
        return weatherMapJpaRepository.findByTimestampBucketLessThanAndIntervalMinutesOrderByTimestampBucketDesc(request.getTimestampBucket(), request.getTimeIntervalInMinutes());
    }
}
