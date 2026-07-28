package me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.persistence.redis;

import lombok.RequiredArgsConstructor;
import me.neobliz1.ecomonitoring.platform.analysis.domain.model.AnalysisConstants;
import me.neobliz1.ecomonitoring.platform.analysis.domain.port.outbound.TelemetryPersistenceRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.time.Duration;

@RequiredArgsConstructor
public class TelemetryPersistenceRepositoryAdapter implements TelemetryPersistenceRepository {

    private final RedisTemplate<String, byte[]> protobufRedisTemplate;
    private final StringRedisTemplate redisTemplate;

    @Value("${spring.redis.records.ttl}")
    private Long redisCacheTtlInterval;

    @Override
    public void saveRealTimeSlidingWindow(String geohashKey, String stationField, String timestampFormatted) {
        String redisKey = AnalysisConstants.WEATHER_HOTWINDOW+geohashKey;
        redisTemplate.opsForHash().put(redisKey, stationField, timestampFormatted);
        redisTemplate.expire(redisKey, Duration.ofHours(redisCacheTtlInterval));
    }

    @Override
    public void saveHistoricalGridCell(String recordKey, String geohash, byte[] serializedLayers) {
        String redisHistoryKey = AnalysisConstants.WEATHER_MAP_KEY+recordKey;
        protobufRedisTemplate.opsForHash().put(redisHistoryKey, geohash, serializedLayers);
        protobufRedisTemplate.expire(redisHistoryKey, Duration.ofHours(redisCacheTtlInterval));
    }
}
