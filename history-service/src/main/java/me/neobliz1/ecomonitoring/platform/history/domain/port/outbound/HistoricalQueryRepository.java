package me.neobliz1.ecomonitoring.platform.history.domain.port.outbound;

import me.neobliz1.ecomonitoring.platform.history.domain.model.entity.WeatherGridCellLayer;
import me.neobliz1.ecomonitoring.platform.history.domain.model.entity.WeatherMapBucket;
import org.jspecify.annotations.NonNull;
import weather.history.SpatialBoxRequest;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface HistoricalQueryRepository {

    Optional<WeatherMapBucket> findByTimestampBucketAndIntervalMinutes(@NonNull SpatialBoxRequest request);

    List<WeatherGridCellLayer> findByBucketIdAndSpatialBox(UUID bucketId, @NonNull SpatialBoxRequest request);

    List<WeatherMapBucket> findAllPastBuckets(@NonNull SpatialBoxRequest request);
}