package me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.persistence.redis;

import static me.neobliz1.ecomonitoring.platform.analysis.domain.model.AnalysisConstants.GRID_BUCKET_KEY_FORMAT;

import io.lettuce.core.RedisNoScriptException;
import lombok.RequiredArgsConstructor;
import me.neobliz1.ecomonitoring.platform.analysis.domain.port.outbound.TelemetryQueryRepository;
import me.neobliz1.ecomonitoring.platform.model.exception.WeatherMapDataNotFoundException;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RequiredArgsConstructor
public class TelemetryQueryRepositoryAdapter implements TelemetryQueryRepository {

    private final RedisScript<List<byte[]>> queryHistoricalGridScript;
    private final RedisTemplate<String, byte[]> protobufRedisTemplate;

    @Override
    public Map<String, byte[]> findFilteredGridDataBySpatialBox(
            long activeBucketFloor,
            double minLat, double maxLat, double minLon, double maxLon) {
        double centerLon = (minLon+maxLon)/2.0;
        double centerLat = (minLat+maxLat)/2.0;
        double widthKm = calculateDistanceKm(centerLat, minLon, centerLat, maxLon);
        double heightKm = calculateDistanceKm(minLat, centerLon, maxLat, centerLon);

        String spatialIndexKey = "spatial_index:"+String.format(GRID_BUCKET_KEY_FORMAT, activeBucketFloor);

        byte[][] keysAndArgs = new byte[][]{
                spatialIndexKey.getBytes(StandardCharsets.UTF_8),             // Index 0 -> KEYS[1]
                String.valueOf(centerLon).getBytes(StandardCharsets.UTF_8),   // Index 1 -> ARGV[1]
                String.valueOf(centerLat).getBytes(StandardCharsets.UTF_8),   // Index 2 -> ARGV[2]
                String.valueOf(widthKm).getBytes(StandardCharsets.UTF_8),     // Index 3 -> ARGV[3]
                String.valueOf(heightKm).getBytes(StandardCharsets.UTF_8)     // Index 4 -> ARGV[4]
        };

        List<byte[]> rawResultList = protobufRedisTemplate.execute((RedisCallback<List<byte[]>>) connection -> {
            try {
                // Try the high-performance SHA path first
                return connection.scriptingCommands().evalSha(
                        queryHistoricalGridScript.getSha1(),
                        ReturnType.MULTI,
                        1,
                        keysAndArgs
                );
            } catch(org.springframework.data.redis.RedisSystemException e) {
                if(e.getRootCause() instanceof RedisNoScriptException
                        || (e.getMessage()!=null && e.getMessage().contains("NOSCRIPT"))) {

                    // Fallback: Send the full script text to re-cache it on Redis
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

        if(rawResultList==null || rawResultList.isEmpty()) {
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
