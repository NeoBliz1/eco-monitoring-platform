package me.neobliz1.ecomonitoring.platform.analysis.domain.service;

import static java.util.Objects.isNull;

import com.google.protobuf.InvalidProtocolBufferException;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import me.neobliz1.ecomonitoring.platform.analysis.domain.port.inbound.TelemetryQueryService;
import me.neobliz1.ecomonitoring.platform.analysis.domain.port.outbound.TelemetryQueryArchive;
import me.neobliz1.ecomonitoring.platform.analysis.domain.port.outbound.TelemetryQueryRepository;
import me.neobliz1.ecomonitoring.platform.model.exception.ProtocolBufferTranslationException;
import me.neobliz1.ecomonitoring.platform.model.exception.WeatherMapDataNotFoundException;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.GridCellLayers;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.WeatherMap;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;

@RequiredArgsConstructor
public class TelemetryStateQueryResolver implements TelemetryQueryService {

    private final TelemetryQueryRepository telemetryQueryRepositoryAdapter;
    private final TelemetryQueryArchive telemetryQueryArchiveAdapter;
    private final int aggregationSecondsPerInterval;
    private final int historyRecordsTtlInHours;

    @Override
    public @NonNull WeatherMap getLatestTimeIntervalWeatherMapByCoordinates(long targetTimestamp, Double minLat, Double maxLat, Double minLon, Double maxLon) {
        long activeBucketFloor = TelemetryUtils.getAggregationBucketFloorInterval(targetTimestamp, aggregationSecondsPerInterval);
        if(Instant.now().minusMillis(activeBucketFloor).toEpochMilli()>Duration.ofHours(historyRecordsTtlInHours).toMillis()) {
            return telemetryQueryArchiveAdapter.findFilteredGridDataBySpatialBoxInArchive(activeBucketFloor, minLat, maxLat, minLon, maxLon);
        }
        Map<String, byte[]> filteredDataMatrix = telemetryQueryRepositoryAdapter.findFilteredGridDataBySpatialBox(
                activeBucketFloor, minLat, maxLat, minLon, maxLon
        );
        if(isNull(filteredDataMatrix) || filteredDataMatrix.isEmpty()) {
            throw new WeatherMapDataNotFoundException();
        }
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