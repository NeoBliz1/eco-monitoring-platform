package me.neobliz1.ecomonitoring.platform.analysis.domain.service;

import com.google.protobuf.InvalidProtocolBufferException;
import lombok.RequiredArgsConstructor;
import me.neobliz1.ecomonitoring.platform.analysis.domain.port.inbound.TelemetryQueryService;
import me.neobliz1.ecomonitoring.platform.analysis.domain.port.outbound.TelemetryQueryRepository;
import me.neobliz1.ecomonitoring.platform.model.exception.ProtocolBufferTranslationException;
import me.neobliz1.ecomonitoring.platform.model.exception.WeatherMapDataNotFoundException;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.GridCellLayers;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.WeatherMap;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.annotation.Cacheable;

import java.time.Duration;
import java.util.Map;

@RequiredArgsConstructor
public class TelemetryStateQueryResolver implements TelemetryQueryService {

    private final TelemetryQueryRepository telemetryQueryRepositoryAdapter;

    @Value("${spring.kafka.streams.pipeline.name.aggregation-processor.interval}")
    private Integer aggregationSecondsPerInterval;
    @Override
    @Cacheable(value = "weatherMaps", key = "#root.args[0] + '#' + #root.args[1] + ',' + #root.args[2] + ',' + #root.args[3] + ',' + #root.args[4]")
    public WeatherMap getLatestTimeIntervalWeatherMapByCoordinates(long targetTimestamp, Double minLat, Double maxLat, Double minLon, Double maxLon) {

        long activeBucketFloor = TelemetryUtils.getAggregationBucketFloorInterval(targetTimestamp, aggregationSecondsPerInterval);
        Map<String, byte[]> filteredDataMatrix = telemetryQueryRepositoryAdapter.findFilteredGridDataBySpatialBox(activeBucketFloor,
                minLat, maxLat, minLon, maxLon);
        WeatherMap.Builder weatherMapBuilder = WeatherMap.newBuilder()
                .setTimestampBucket(activeBucketFloor)
                .setIntervalMinutes((int) Duration.ofSeconds(aggregationSecondsPerInterval).toMinutes());
        filteredDataMatrix.forEach((key, valueBytes) -> {
            try {
                GridCellLayers gridCellLayers = GridCellLayers.parseFrom(valueBytes);
                weatherMapBuilder.putGridCells(key, gridCellLayers);
            } catch(InvalidProtocolBufferException e) {
                throw new ProtocolBufferTranslationException("Corrupted Protobuf payload for grid cell: "+key, e);
            }
        });
        if(weatherMapBuilder.getGridCellsMap().isEmpty()) {
            throw new WeatherMapDataNotFoundException();
        }
        return weatherMapBuilder.build();
    }
}
