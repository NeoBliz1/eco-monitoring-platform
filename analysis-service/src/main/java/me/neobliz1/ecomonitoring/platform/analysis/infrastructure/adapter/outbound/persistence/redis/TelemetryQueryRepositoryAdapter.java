package me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.persistence.redis;

import static me.neobliz1.ecomonitoring.platform.analysis.domain.service.AnalysisUtils.getAggregationBucketFloorMillisInterval;
import static me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.messaging.kafka.processor.util.AggregationUtils.getAggregationMillisPerInterval;
import static me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.messaging.kafka.processor.util.AggregationUtils.getAggregationSecondsPerInterval;
import static me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.messaging.kafka.processor.util.AggregationUtils.getRedisCacheHoursTtlInterval;
import static me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.messaging.kafka.processor.util.AggregationUtils.getRedisCacheMillisTtlInterval;

import com.google.protobuf.InvalidProtocolBufferException;
import io.lettuce.core.RedisNoScriptException;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import me.neobliz1.ecomonitoring.platform.analysis.domain.model.dto.WeatherMapAnalysisRequestQuery;
import me.neobliz1.ecomonitoring.platform.analysis.domain.port.outbound.TelemetryPersistenceRepository;
import me.neobliz1.ecomonitoring.platform.analysis.domain.port.outbound.TelemetryQueryArchive;
import me.neobliz1.ecomonitoring.platform.analysis.domain.port.outbound.TelemetryQueryRepository;
import me.neobliz1.ecomonitoring.platform.analysis.domain.service.AnalysisUtils;
import me.neobliz1.ecomonitoring.platform.analysis.domain.service.SpatialRequestValidator;
import me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.messaging.kafka.record.ParsedStorageKey;
import me.neobliz1.ecomonitoring.platform.analysis.infrastructure.config.AnalysisInfrastructureProperties;
import me.neobliz1.ecomonitoring.platform.common.util.PlatformCommonUtils;
import me.neobliz1.ecomonitoring.platform.model.exception.ProtocolBufferTranslationException;
import me.neobliz1.ecomonitoring.platform.model.exception.WeatherMapDataNotFoundException;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.GridCellLayers;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.WeatherMap;
import org.jspecify.annotations.NonNull;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import weather.history.SpatialBoxRequest;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@RequiredArgsConstructor
public class TelemetryQueryRepositoryAdapter implements TelemetryQueryRepository {

    private final TelemetryPersistenceRepository telemetryPersistenceRepository;
    private final RedisTemplate<String, byte[]> protobufRedisTemplate;
    private final RedisScript<List<byte[]>> queryHistoricalGridScript;
    private final TelemetryQueryArchive telemetryQueryArchiveAdapter;
    private final AnalysisInfrastructureProperties props;

    private Integer aggregationSecondsPerInterval;
    private Integer redisCacheHoursTtlInterval;
    private long aggregationMillisPerInterval;
    private long redisCacheMillisTtlInterval;
    private long possibleBucketInterval;

    private static boolean rawResultsIsEmpty(List<byte[]> rawResultList) {
        return rawResultList.isEmpty() || rawResultList.size()%2!=0;
    }

    private static @NonNull Map<String, byte[]> convertResultListToMap(List<byte[]> rawResultList) {
        Map<String, byte[]> filteredMatrix = new HashMap<>(rawResultList.size()/2);
        for(int i = 0; i<rawResultList.size(); i += 2) {
            String key = new String(rawResultList.get(i), StandardCharsets.UTF_8);
            byte[] binaryPayload = rawResultList.get(i+1);
            filteredMatrix.put(key, binaryPayload);
        }
        return filteredMatrix;
    }

    private static GridCellLayers parseValueBytes(String key, byte[] valueBytes) {
        try {
            return GridCellLayers.parseFrom(valueBytes);
        } catch(InvalidProtocolBufferException e) {
            throw new ProtocolBufferTranslationException("Corrupted Protobuf payload for grid cell: "+key, e);
        }
    }

    private static WeatherMapAnalysisRequestQuery expandRequestSpatialBoxByTier(WeatherMapAnalysisRequestQuery request, int tier) {
        SpatialBoxRequest expandedRequest = SpatialBoxRequest.newBuilder()
                .setTimestampBucket(request.targetTimestamp())
                .setMinLat(request.minLat())
                .setMaxLat(request.maxLat())
                .setMinLon(request.minLon())
                .setMaxLon(request.maxLon())
                .build();
        SpatialBoxRequest spatialBoxRequest = PlatformCommonUtils.expandRequestSpatialBoxByTier(expandedRequest, tier);
        return new WeatherMapAnalysisRequestQuery(
                spatialBoxRequest.getTimestampBucket(),
                spatialBoxRequest.getMinLat(),
                spatialBoxRequest.getMaxLat(),
                spatialBoxRequest.getMinLon(),
                spatialBoxRequest.getMaxLon());
    }

    @PostConstruct
    public void postConstructInit() {
        this.aggregationMillisPerInterval = getAggregationMillisPerInterval(props);
        this.aggregationSecondsPerInterval = getAggregationSecondsPerInterval(props);
        this.redisCacheMillisTtlInterval = getRedisCacheMillisTtlInterval(props);
        this.redisCacheHoursTtlInterval = getRedisCacheHoursTtlInterval(props);
    }

    @Override
    public @NonNull WeatherMap getWeatherMapByTimestampAndSpatialBox(@NonNull WeatherMapAnalysisRequestQuery request) {
        SpatialRequestValidator.validateCoordinatesBox(request);
        long aggregationBucketFloorMillisInterval = getAggregationBucketFloorMillisInterval(request.targetTimestamp(), aggregationSecondsPerInterval);
        WeatherMapAnalysisRequestQuery requestWithFloorTimestamp = request.withTargetTimestamp(aggregationBucketFloorMillisInterval);
        Map<String, byte[]> filteredDataMatrix = findFilteredGridDataBySpatialBoxInRedis(requestWithFloorTimestamp);
        if(filteredDataMatrix.isEmpty()) {
            return getWeatherMapBySpatialBoxFromHistoryService(requestWithFloorTimestamp);
        }
        return buildWeatherMapFromDataMatrix(aggregationBucketFloorMillisInterval, filteredDataMatrix);
    }

    public @NonNull Map<String, byte[]> findFilteredGridDataBySpatialBoxInRedis(@NonNull WeatherMapAnalysisRequestQuery request) {
        List<byte[]> rawResultList = List.of();
        possibleBucketInterval = request.targetTimestamp()-redisCacheMillisTtlInterval;
        WeatherMapAnalysisRequestQuery expandedRequest = request;
        for(int i = 0; i<4; i++) {
            rawResultList = findDataByLastExistsTimestampInRedis(expandedRequest);
            if(!rawResultList.isEmpty()) {
                break;
            }
            expandedRequest = expandRequestSpatialBoxByTier(expandedRequest, i);
        }
        return convertResultListToMap(rawResultList);
    }

    private @NonNull List<byte[]> findDataByLastExistsTimestampInRedis(@NonNull WeatherMapAnalysisRequestQuery request) {
        List<byte[]> rawResults = List.of();
        try {
            long targetTimestamp = request.targetTimestamp();
            while(rawResultsIsEmpty(rawResults) && targetTimestamp>possibleBucketInterval) {
                byte[][] keysAndArgs = parseArgsForSearchInRedis(request.withTargetTimestamp(targetTimestamp));
                rawResults = executeRedisRequest(keysAndArgs);
                long oneStepPastTimestamp = targetTimestamp-aggregationMillisPerInterval;
                targetTimestamp = getAggregationBucketFloorMillisInterval(oneStepPastTimestamp, aggregationSecondsPerInterval);
            }
            if(rawResultsIsEmpty(rawResults)) {
                throw new WeatherMapDataNotFoundException();
            }
        } catch(WeatherMapDataNotFoundException e) {
            if(log.isDebugEnabled()) {
                log.debug("No data found for bucket {}. Expand spatial box...", request.targetTimestamp());
            }
        }
        return rawResults;
    }

    private byte[] @NonNull [] parseArgsForSearchInRedis(@NonNull WeatherMapAnalysisRequestQuery request) {
        Double minLat = request.minLat();
        Double maxLat = request.maxLat();
        Double minLon = request.minLon();
        Double maxLon = request.maxLon();
        double centerLat = (minLat+maxLat)/2.0;
        double centerLon = (minLon+maxLon)/2.0;
        double widthKm = calculateDistanceKm(centerLat, minLon, centerLat, maxLon);
        double heightKm = calculateDistanceKm(minLat, centerLon, maxLat, centerLon);
        String spatialIndexKey = AnalysisUtils.addSpatialIndexPrefixToTargetFormattedTimestamp(request.targetTimestamp());
        long ttlInSeconds = Duration.ofHours(redisCacheHoursTtlInterval).toSeconds();
        return new byte[][]{
                spatialIndexKey.getBytes(StandardCharsets.UTF_8),             // Index 0 -> KEYS[1]
                String.valueOf(centerLon).getBytes(StandardCharsets.UTF_8),   // Index 1 -> ARGV[1]
                String.valueOf(centerLat).getBytes(StandardCharsets.UTF_8),   // Index 2 -> ARGV[2]
                String.valueOf(widthKm).getBytes(StandardCharsets.UTF_8),     // Index 3 -> ARGV[3]
                String.valueOf(heightKm).getBytes(StandardCharsets.UTF_8),    // Index 4 -> ARGV[4]
                String.valueOf(ttlInSeconds).getBytes(StandardCharsets.UTF_8) // Index 5 -> ARGV[5]
        };
    }

    private @NonNull List<byte[]> executeRedisRequest(byte[][] keysAndArgs) {
        return protobufRedisTemplate.execute((RedisCallback<List<byte[]>>) connection ->
                executeLuaScripts(connection, keysAndArgs));
    }

    private double calculateDistanceKm(double lat1, double lon1, double lat2, double lon2) {
        final int EARTH_RADIUS_KM = 6371;
        double latDistance = Math.toRadians(lat2-lat1);
        double lonDistance = Math.toRadians(lon2-lon1);
        double a = Math.sin(latDistance/2)*Math.sin(latDistance/2)
                +Math.cos(Math.toRadians(lat1))*Math.cos(Math.toRadians(lat2))
                *Math.sin(lonDistance/2)*Math.sin(lonDistance/2);
        double c = 2*Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
        return EARTH_RADIUS_KM*c;
    }

    private List<byte[]> executeLuaScripts(RedisConnection connection, byte[][] keysAndArgs) {
        try {
            return connection.scriptingCommands().evalSha(
                    queryHistoricalGridScript.getSha1(),
                    ReturnType.MULTI,
                    1,
                    keysAndArgs
            );
        } catch(RedisSystemException e) {
            if(e.getRootCause() instanceof RedisNoScriptException
                    || (e.getMessage()!=null && e.getMessage().contains("NOSCRIPT"))) {
                if(log.isDebugEnabled()) {
                    log.debug("Redis script cache miss (NOSCRIPT). Send the full script text to re-cache it on Redis.");
                }
                byte[] rawScriptBytes = queryHistoricalGridScript.getScriptAsString().getBytes(StandardCharsets.UTF_8);
                return connection.scriptingCommands().eval(
                        rawScriptBytes,
                        ReturnType.MULTI,
                        1,
                        keysAndArgs
                );
            }
            throw e;
        }
    }

    private @NonNull WeatherMap getWeatherMapBySpatialBoxFromHistoryService(@NonNull WeatherMapAnalysisRequestQuery request) {
        WeatherMap weatherMap = telemetryQueryArchiveAdapter.findGridDataBySpatialBoxInHistoryService(request);
        if(!weatherMap.getGridCellsMap().isEmpty()) {
            weatherMap.getGridCellsMap().forEach((geohash, gridCellLayers) -> {
                try {
                    ParsedStorageKey parsedKey = new ParsedStorageKey(String.valueOf(weatherMap.getTimestampBucket()), geohash, null);
                    telemetryPersistenceRepository.saveHistoricalGridCellLayer(parsedKey.spatialKey(), gridCellLayers.toByteArray());
                } catch(Exception e) {
                    log.warn("Failed to repopulate Redis cache for geohash index [{}]: {}", geohash, e.getMessage());
                }
            });
        } else {
            throw new WeatherMapDataNotFoundException();
        }
        return weatherMap;
    }

    private @NonNull WeatherMap buildWeatherMapFromDataMatrix(long activeBucketFloor, Map<String, byte[]> filteredDataMatrix) {
        WeatherMap.Builder weatherMapBuilder = WeatherMap.newBuilder()
                .setTimestampBucket(activeBucketFloor)
                .setIntervalMinutes((int) Duration.ofSeconds(aggregationSecondsPerInterval).toMinutes());
        filteredDataMatrix.forEach((key, valueBytes) -> {
            if(valueBytes==null) return;
            GridCellLayers gridCellLayers = parseValueBytes(key, valueBytes);
            weatherMapBuilder.putGridCells(gridCellLayers.getGeohash(), gridCellLayers);
        });
        if(weatherMapBuilder.getGridCellsMap().isEmpty()) {
            throw new WeatherMapDataNotFoundException();
        }
        return weatherMapBuilder.build();
    }
}
