package me.neobliz1.ecomonitoring.platform.analysis.infrastructure.config;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import me.neobliz1.ecomonitoring.platform.analysis.domain.port.outbound.TelemetryPersistenceRepository;
import me.neobliz1.ecomonitoring.platform.analysis.domain.port.outbound.TelemetryQueryRepository;
import me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.persistence.redis.TelemetryPersistenceRepositoryAdapter;
import me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.persistence.redis.TelemetryQueryRepositoryAdapter;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;

import java.util.List;

@Slf4j
@RequiredArgsConstructor
public class RedisTestConfig {

    @Bean
    public RedisScript<String> saveHistoricalGridScript() {
        return RedisScript.of(new ClassPathResource("lua/scripts/save_historical_grid.lua"));
    }

    @Bean
    @SuppressWarnings("unchecked")
    public RedisScript<List<byte[]>> queryHistoricalGridScript() {
        return RedisScript.of(new ClassPathResource("lua/scripts/query_historical_grid.lua"), (Class<List<byte[]>>) (Class<?>) List.class);
    }

    @Bean
    public TelemetryPersistenceRepository telemetryPersistenceRepository(ReactiveStringRedisTemplate reactiveStringRedisTemplate,
                                                                         RedisTemplate<String, byte[]> protobufRedisTemplate,
                                                                         RedisScript<String> saveHistoricalGridScript) {
        return new TelemetryPersistenceRepositoryAdapter(reactiveStringRedisTemplate, protobufRedisTemplate, saveHistoricalGridScript);
    }

    @Bean
    public TelemetryQueryRepository telemetryQueryRepository(RedisTemplate<String, byte[]> protobufRedisTemplate,
                                                             RedisScript<List<byte[]>> queryHistoricalGridScript) {
        return new TelemetryQueryRepositoryAdapter(queryHistoricalGridScript, protobufRedisTemplate);
    }
}