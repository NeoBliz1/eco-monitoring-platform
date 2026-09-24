package me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.persistence.redis;

import static java.util.Objects.isNull;

import com.google.protobuf.InvalidProtocolBufferException;
import io.lettuce.core.RedisNoScriptException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import me.neobliz1.ecomonitoring.platform.analysis.domain.port.outbound.TelemetryQueryArchive;
import me.neobliz1.ecomonitoring.platform.analysis.domain.port.outbound.TelemetryQueryRepository;
import me.neobliz1.ecomonitoring.platform.analysis.domain.service.TelemetryUtils;
import me.neobliz1.ecomonitoring.platform.model.exception.ProtocolBufferTranslationException;
import me.neobliz1.ecomonitoring.platform.model.exception.WeatherMapDataNotFoundException;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.GridCellLayers;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.WeatherMap;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@RequiredArgsConstructor
public class TelemetryQueryRepositoryAdapter implements TelemetryQueryRepository {

    private final RedisScript<List<byte[]>> queryHistoricalGridScript;
    private final RedisTemplate<String, byte[]> protobufRedisTemplate;
    private final TelemetryQueryArchive telemetryQueryArchiveAdapter;
    private final Integer aggregationSecondsPerInterval;

    @Override
    public WeatherMap getWeatherMapByTimestampAndSpatialBox(long targetTimestamp, Double minLat, Double maxLat, Double minLon, Double maxLon) {
        long activeBucketFloor = TelemetryUtils.getAggregationBucketFloorInterval(targetTimestamp, aggregationSecondsPerInterval);
        Map<String, byte[]> filteredDataMatrix = null;
        long loopBucketTime = activeBucketFloor;
        long intervalMillis = TelemetryUtils.getMillis(aggregationSecondsPerInterval);
        while(filteredDataMatrix==null && hasSpatialIndexKey(loopBucketTime)) {
            filteredDataMatrix = findFilteredGridDataBySpatialBoxInRedis(loopBucketTime, minLat, maxLat, minLon, maxLon);
            loopBucketTime -= intervalMillis;
        }
        if(isNull(filteredDataMatrix) || filteredDataMatrix.isEmpty()) {
            return telemetryQueryArchiveAdapter.findGridDataBySpatialBoxInHistoryService(activeBucketFloor, minLat, maxLat, minLon, maxLon);
        }
        WeatherMap.Builder weatherMapBuilder = WeatherMap.newBuilder()
                .setTimestampBucket(activeBucketFloor)
                .setIntervalMinutes((int) Duration.ofSeconds(aggregationSecondsPerInterval).toMinutes());
        filteredDataMatrix.forEach((key, valueBytes) -> {
            try {
                if(valueBytes==null) return;
                GridCellLayers gridCellLayers = GridCellLayers.parseFrom(valueBytes);
                weatherMapBuilder.putGridCells(gridCellLayers.getGeohash(), gridCellLayers);
            } catch(InvalidProtocolBufferException e) {
                throw new ProtocolBufferTranslationException("Corrupted Protobuf payload for grid cell: "+key, e);
            }
        });
        if(weatherMapBuilder.getGridCellsMap().isEmpty()) {
            throw new WeatherMapDataNotFoundException();
        }
        return weatherMapBuilder.build();
    }

    private boolean hasSpatialIndexKey(long activeBucketFloor) {
        String spatialIndexKey = TelemetryUtils.getSpatialIndexKey(activeBucketFloor);
        return protobufRedisTemplate.hasKey(spatialIndexKey);
    }

    public Map<String, byte[]> findFilteredGridDataBySpatialBoxInRedis(
            long activeBucketFloor,
            double minLat, double maxLat, double minLon, double maxLon) {
        double centerLon = (minLon+maxLon)/2.0;
        double centerLat = (minLat+maxLat)/2.0;
        double widthKm = calculateDistanceKm(centerLat, minLon, centerLat, maxLon);
        double heightKm = calculateDistanceKm(minLat, centerLon, maxLat, centerLon);
        String spatialIndexKey = TelemetryUtils.getSpatialIndexKey(activeBucketFloor);
        byte[][] keysAndArgs = new byte[][]{
                spatialIndexKey.getBytes(StandardCharsets.UTF_8),             // Index 0 -> KEYS[1]
                String.valueOf(centerLon).getBytes(StandardCharsets.UTF_8),   // Index 1 -> ARGV[1]
                String.valueOf(centerLat).getBytes(StandardCharsets.UTF_8),   // Index 2 -> ARGV[2]
                String.valueOf(widthKm).getBytes(StandardCharsets.UTF_8),     // Index 3 -> ARGV[3]
                String.valueOf(heightKm).getBytes(StandardCharsets.UTF_8)     // Index 4 -> ARGV[4]
        };
        long startTime = 0;
        if(log.isDebugEnabled()) {
            log.debug("Executing Redis script. SHA1: {}, Key: {}, CenterLon: {}, CenterLat: {}, WidthKm: {}, HeightKm: {}",
                    queryHistoricalGridScript.getSha1(), spatialIndexKey, centerLon, centerLat, widthKm, heightKm);
            startTime = System.currentTimeMillis();
        }
        List<byte[]> rawResultList = protobufRedisTemplate.execute((RedisCallback<List<byte[]>>) connection -> {
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
        });
        if(log.isDebugEnabled()) {
            long duration = System.currentTimeMillis()-startTime;
            log.debug("Redis script execution completed in {} ms. Raw result count: {}",
                    duration, (rawResultList!=null?rawResultList.size():0));
        }
        if(rawResultList==null || rawResultList.isEmpty() || rawResultList.size()%2!=0) {
            throw new WeatherMapDataNotFoundException();
        }

        Map<String, byte[]> filteredMatrix = new HashMap<>(rawResultList.size()/2);
        for(int i = 0; i<rawResultList.size(); i += 2) {
            String key = new String(rawResultList.get(i), StandardCharsets.UTF_8);
            byte[] binaryPayload = rawResultList.get(i+1);
            filteredMatrix.put(key, binaryPayload);
        }

        return filteredMatrix;
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
}
