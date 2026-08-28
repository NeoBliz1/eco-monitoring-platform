package me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.persistence.redis;

import static me.neobliz1.ecomonitoring.platform.common.constant.PlatformConstants.HASHTAG_DELIMITER;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import me.neobliz1.ecomonitoring.platform.analysis.domain.model.AnalysisConstants;
import me.neobliz1.ecomonitoring.platform.analysis.domain.port.outbound.TelemetryPersistenceRepository;
import org.jspecify.annotations.NonNull;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
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
    private final RedisScript<String> saveHistoricalGridScript;

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
    public void saveHistoricalGridCell(String geohash, byte[] serializedLayers) {
        String[] parts = geohash.split(HASHTAG_DELIMITER);
        byte[][] scriptArgs = parseArgsForStoreInRedis(parts[0], geohash, serializedLayers);
        protobufRedisTemplate.execute(
                saveHistoricalGridScript,
                RedisSerializer.byteArray(),
                RedisSerializer.string(),
                List.of(geohash),
                (Object[]) scriptArgs
        );
    }

    private byte[] @NonNull [] parseArgsForStoreInRedis(String recordKey, String geohash, byte[] serializedLayers) {
        String[] parts = geohash.split(HASHTAG_DELIMITER);
        String lat = parts[1];
        String lon = parts[2];
        long ttlInSeconds = Duration.ofHours(redisCacheTtlInterval).toSeconds();
        return new byte[][]{
                lat.getBytes(StandardCharsets.UTF_8),
                lon.getBytes(StandardCharsets.UTF_8),
                serializedLayers,
                String.valueOf(ttlInSeconds).getBytes(StandardCharsets.UTF_8),
                recordKey.getBytes(StandardCharsets.UTF_8)
        };
    }
}

