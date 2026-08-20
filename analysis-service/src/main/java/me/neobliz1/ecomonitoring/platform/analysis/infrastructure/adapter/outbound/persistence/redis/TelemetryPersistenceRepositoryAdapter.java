package me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.persistence.redis;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import me.neobliz1.ecomonitoring.platform.analysis.domain.model.AnalysisConstants;
import me.neobliz1.ecomonitoring.platform.analysis.domain.port.outbound.TelemetryPersistenceRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.serializer.RedisSerializer;
import reactor.core.scheduler.Schedulers;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;

@Slf4j
@RequiredArgsConstructor
public class TelemetryPersistenceRepositoryAdapter implements TelemetryPersistenceRepository {

    private final ReactiveStringRedisTemplate reactiveStringRedisTemplate;
    private final RedisTemplate<String, byte[]> protobufRedisTemplate;
    private final DefaultRedisScript<String> saveHistoricalGridScript;

    @Value("${spring.redis.records.ttl}")
    private Long redisCacheTtlInterval;

    @Override
    public void saveRealTimeSlidingWindow(String geohashKey, String stationField, String timestampFormatted) {
        String redisKey = AnalysisConstants.WEATHER_HOTWINDOW+geohashKey;
        Duration ttl = Duration.ofHours(redisCacheTtlInterval);

        reactiveStringRedisTemplate.opsForHash()
                .put(redisKey, stationField, timestampFormatted)
                .then(reactiveStringRedisTemplate.expire(redisKey, ttl))
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(
                        success -> {
                            if(log.isDebugEnabled())
                                log.debug("Sliding window geohash: {}, station: {}, timestamp: {} successfully written",
                                        geohashKey, stationField, timestampFormatted);
                        },
                        error -> log.warn("Sliding window geohash: {}, station: {}, timestamp: {}, {}",
                                geohashKey, stationField, timestampFormatted, error.getMessage())
                );
    }

    @Override
    public void saveHistoricalGridCell(String recordKey, String geohash, byte[] serializedLayers) {
        String redisHistoryKey = AnalysisConstants.WEATHER_MAP_KEY+recordKey;
        byte[] geohashBytes = geohash.getBytes(StandardCharsets.UTF_8);
        byte[] ttlBytes = String.valueOf(redisCacheTtlInterval).getBytes(StandardCharsets.UTF_8);
        byte[][] scriptArgs = new byte[][]{ geohashBytes, serializedLayers, ttlBytes };

        protobufRedisTemplate.execute(
                saveHistoricalGridScript,
                RedisSerializer.byteArray(),
                RedisSerializer.string(),
                List.of(redisHistoryKey),
                (Object[]) scriptArgs
        );
    }
}

