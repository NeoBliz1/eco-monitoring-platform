package me.neobliz1.ecomonitoring.platform.analysis.domain.service;

import static me.neobliz1.ecomonitoring.platform.common.constant.PlatformConstants.HASHTAG_DELIMITER;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import lombok.RequiredArgsConstructor;
import me.neobliz1.ecomonitoring.platform.analysis.domain.model.AnalysisConstants;
import me.neobliz1.ecomonitoring.platform.analysis.domain.port.inbound.TelemetryQueryService;
import me.neobliz1.ecomonitoring.platform.analysis.domain.port.outbound.TelemetryQueryRepository;
import me.neobliz1.ecomonitoring.platform.model.exception.InvalidCoordinatesSquareException;
import me.neobliz1.ecomonitoring.platform.model.exception.ProtocolBufferTranslationException;
import me.neobliz1.ecomonitoring.platform.model.exception.WeatherMapDataNotFoundException;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.GridCellLayers;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.WeatherMap;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.annotation.Cacheable;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@RequiredArgsConstructor
public class TelemetryStateQueryResolver implements TelemetryQueryService {

    private final TelemetryQueryRepository telemetryQueryRepositoryAdapter;

    @Value("${spring.kafka.streams.pipeline.name.aggregation-processor.interval}")
    private Integer aggregationSecondsPerInterval;

    @Override
    @Cacheable(value = "weatherMaps", key = "#root.args[0] + '#' + #root.args[1].toString()")
    public String getLatestTimeIntervalWeatherMapByCoordinates(long targetTimestamp, List<Double> coordinatesSquare) {
        if(coordinatesSquare==null || coordinatesSquare.size()<4) {
            throw new InvalidCoordinatesSquareException();
        }
        double minLat = coordinatesSquare.get(0);
        double maxLat = coordinatesSquare.get(1);
        double minLon = coordinatesSquare.get(2);
        double maxLon = coordinatesSquare.get(3);
        boolean isSquare = (maxLat>minLat && maxLon>minLon);
        if(!isSquare) throw new InvalidCoordinatesSquareException();
        long activeBucketFloor = TelemetryUtils.getAggregationBucketFloorInterval(targetTimestamp, aggregationSecondsPerInterval);
        String historyKey = AnalysisConstants.WEATHER_MAP_KEY+activeBucketFloor;
        Map<Object, Object> rawHashDataMatrix = telemetryQueryRepositoryAdapter.findRawGridDataByBucketFloor(historyKey);
        WeatherMap.Builder weatherMapBuilder = WeatherMap.newBuilder()
                .setTimestampBucket(activeBucketFloor)
                .setIntervalMinutes((int) Duration.ofSeconds(aggregationSecondsPerInterval).toMinutes());
        rawHashDataMatrix.entrySet().stream()
                .filter(cellEntry -> {
                    String[] parts = String.valueOf(cellEntry.getKey()).split(HASHTAG_DELIMITER);
                    double cellLat;
                    double cellLon;
                    try {
                        cellLat = Double.parseDouble(parts[1]);
                        cellLon = Double.parseDouble(parts[2]);
                    } catch(NumberFormatException|IndexOutOfBoundsException e) {
                        throw new ProtocolBufferTranslationException("Corrupted or malformed grid matrix coordinates key in Redis storage: "
                                +Arrays.toString(parts), e);
                    }
                    return cellLat>=minLat && cellLat<=maxLat && cellLon>=minLon && cellLon<=maxLon;
                })
                .forEach(entry -> {
                    try {
                        GridCellLayers gridCellLayers = GridCellLayers.parseFrom((byte[]) entry.getValue());
                        weatherMapBuilder.putGridCells(String.valueOf(entry.getKey()), gridCellLayers);
                    } catch(InvalidProtocolBufferException e) {
                        throw new ProtocolBufferTranslationException(e);
                    }
                });
        if(weatherMapBuilder.getGridCellsMap().isEmpty()) {
            throw new WeatherMapDataNotFoundException();
        }
        String result;
        try {
            result = JsonFormat.printer()
                    .alwaysPrintFieldsWithNoPresence()
                    .preservingProtoFieldNames()
                    .print(weatherMapBuilder.build());
        } catch(Exception e) {
            throw new ProtocolBufferTranslationException(e);
        }
        return result;
    }
}
