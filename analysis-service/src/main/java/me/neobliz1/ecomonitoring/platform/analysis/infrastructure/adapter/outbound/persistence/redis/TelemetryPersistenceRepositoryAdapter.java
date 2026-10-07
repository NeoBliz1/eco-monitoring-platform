package me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.persistence.redis;

import static me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.messaging.kafka.processor.util.AggregationUtils.getRedisCacheHoursTtlInterval;
import static me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.messaging.kafka.record.ParsedStorageKey.parseSpatialKey;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import me.neobliz1.ecomonitoring.platform.analysis.domain.model.AnalysisConstants;
import me.neobliz1.ecomonitoring.platform.analysis.domain.port.outbound.TelemetryPersistenceRepository;
import me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.messaging.kafka.record.ParsedStorageKey;
import me.neobliz1.ecomonitoring.platform.analysis.infrastructure.config.AnalysisInfrastructureProperties;
import org.jspecify.annotations.NonNull;
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
    private final AnalysisInfrastructureProperties props;

    private Duration redisCacheHoursTtlInterval;

    @PostConstruct
    public void postConstructInit() {
        this.redisCacheHoursTtlInterval = Duration.ofHours(getRedisCacheHoursTtlInterval(props));
    }

    @Override
    public void saveRealTimeSlidingWindow(String geohashKey, String stationField, String timestampFormatted) {
        String redisKey = AnalysisConstants.WEATHER_HOTWINDOW+geohashKey;
        reactiveStringRedisTemplate.opsForHash()
                .put(redisKey, stationField, timestampFormatted)
                .flatMap(success -> reactiveStringRedisTemplate.expire(redisKey, redisCacheHoursTtlInterval))
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(
                        success -> {
                            if(log.isDebugEnabled()) {
                                log.debug("Sliding window geohash: {}, station: {}, timestamp: {} successfully written",
                                        geohashKey, stationField, timestampFormatted);
                            }
                        },
                        error -> log.warn("Sliding window geohash: {}, station: {}, timestamp: {}, {}",
                                geohashKey, stationField, timestampFormatted, error.getMessage())
                );
    }

    @Override
    public void saveHistoricalGridCellLayer(@NonNull String spatialKey, byte @NonNull [] serializedLayers) {
        ParsedStorageKey parsedSpatialKey = parseSpatialKey(spatialKey);
        byte[][] scriptArgs = parseArgsForStoreInRedis(parsedSpatialKey.bucketTime(), parsedSpatialKey, serializedLayers);
        protobufRedisTemplate.execute(
                saveHistoricalGridScript,
                RedisSerializer.byteArray(),
                RedisSerializer.string(),
                List.of(spatialKey),
                (Object[]) scriptArgs
        );
    }

    private byte[] @NonNull [] parseArgsForStoreInRedis(@NonNull String recordKey, @NonNull ParsedStorageKey parseSpatialKey,
                                                        byte @NonNull [] serializedLayers) {
        String lat = parseSpatialKey.lat();
        String lon = parseSpatialKey.lon();
        long ttlInSeconds = redisCacheHoursTtlInterval.toSeconds();
        return new byte[][]{
                lat.getBytes(StandardCharsets.UTF_8),
                lon.getBytes(StandardCharsets.UTF_8),
                serializedLayers,
                String.valueOf(ttlInSeconds).getBytes(StandardCharsets.UTF_8),
                recordKey.getBytes(StandardCharsets.UTF_8)
        };
    }
}

