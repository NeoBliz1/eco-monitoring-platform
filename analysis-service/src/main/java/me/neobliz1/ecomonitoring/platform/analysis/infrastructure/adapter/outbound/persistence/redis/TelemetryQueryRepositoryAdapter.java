package me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.persistence.redis;

import lombok.RequiredArgsConstructor;
import me.neobliz1.ecomonitoring.platform.analysis.domain.port.outbound.TelemetryQueryRepository;
import me.neobliz1.ecomonitoring.platform.model.exception.WeatherMapDataNotFoundException;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ScanOptions;

import java.util.HashMap;
import java.util.Map;

@RequiredArgsConstructor
public class TelemetryQueryRepositoryAdapter implements TelemetryQueryRepository {

    private final RedisTemplate<String, byte[]> protobufRedisTemplate;

    @Override
    public Map<Object, Object> findRawGridDataByBucketFloor(String historyKey) {
        Map<Object, Object> rawHashDataMatrix = new HashMap<>();
        ScanOptions options = ScanOptions.scanOptions().count(250).build();
        try(Cursor<Map.Entry<Object, Object>> cursor = protobufRedisTemplate.opsForHash().scan(historyKey, options)) {
            while(cursor.hasNext()) {
                Map.Entry<Object, Object> entry = cursor.next();
                rawHashDataMatrix.put(entry.getKey(), entry.getValue());
            }
        } catch(Exception e) {
            throw new RuntimeException("Failed to safely scan telemetry grid matrix from Redis", e);
        }

        if(rawHashDataMatrix.isEmpty()) {
            throw new WeatherMapDataNotFoundException();
        }

        return rawHashDataMatrix;
    }
}