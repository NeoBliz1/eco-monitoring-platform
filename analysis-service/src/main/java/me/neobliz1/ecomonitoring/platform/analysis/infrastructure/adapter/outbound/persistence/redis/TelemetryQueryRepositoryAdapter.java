package me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.persistence.redis;

import lombok.RequiredArgsConstructor;
import me.neobliz1.ecomonitoring.platform.analysis.domain.port.outbound.TelemetryQueryRepository;
import me.neobliz1.ecomonitoring.platform.model.exception.WeatherMapDataNotFoundException;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.Map;

@RequiredArgsConstructor
public class TelemetryQueryRepositoryAdapter implements TelemetryQueryRepository {

    private final RedisTemplate<String, byte[]> protobufRedisTemplate;

    @Override
    public Map<Object, Object> findRawGridDataByBucketFloor(String historyKey) {
        Map<Object, Object> rawHashDataMatrix = protobufRedisTemplate.opsForHash().entries(historyKey);
        if(rawHashDataMatrix==null || rawHashDataMatrix.isEmpty()) {
            throw new WeatherMapDataNotFoundException();
        }
        return rawHashDataMatrix;
    }
}
