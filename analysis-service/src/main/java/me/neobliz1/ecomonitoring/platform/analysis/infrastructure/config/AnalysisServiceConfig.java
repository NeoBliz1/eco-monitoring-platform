package me.neobliz1.ecomonitoring.platform.analysis.infrastructure.config;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import me.neobliz1.ecomonitoring.platform.analysis.domain.port.inbound.TelemetryQueryService;
import me.neobliz1.ecomonitoring.platform.analysis.domain.port.outbound.TelemetryPersistenceRepository;
import me.neobliz1.ecomonitoring.platform.analysis.domain.port.outbound.TelemetryPersistentService;
import me.neobliz1.ecomonitoring.platform.analysis.domain.port.outbound.TelemetryQueryArchive;
import me.neobliz1.ecomonitoring.platform.analysis.domain.port.outbound.TelemetryQueryRepository;
import me.neobliz1.ecomonitoring.platform.analysis.domain.service.TelemetryStatePersister;
import me.neobliz1.ecomonitoring.platform.analysis.domain.service.TelemetryStateQueryResolver;
import me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.history.grpc.TelemetryQueryGrpcAdapter;
import me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.persistence.redis.TelemetryPersistenceRepositoryAdapter;
import me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.persistence.redis.TelemetryQueryRepositoryAdapter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.grpc.client.ImportGrpcClients;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import weather.history.HistoryServiceGrpc;

import java.util.List;

@Slf4j
@Configuration
@EnableKafkaStreams
@RequiredArgsConstructor
@ImportGrpcClients(target = "history-service", types = HistoryServiceGrpc.HistoryServiceBlockingStub.class)
public class AnalysisServiceConfig {

    private final AnalysisInfrastructureProperties props;

    @Bean
    public TelemetryPersistentService telemetryPersistentService(TelemetryPersistenceRepository telemetryRepository) {
        return new TelemetryStatePersister(telemetryRepository, props);
    }

    @Bean
    public TelemetryPersistenceRepository telemetryPersistenceRepository(ReactiveStringRedisTemplate reactiveStringRedisTemplate,
                                                                         RedisTemplate<String, byte[]> protobufRedisTemplate,
                                                                         RedisScript<String> saveHistoricalGridScript) {
        return new TelemetryPersistenceRepositoryAdapter(reactiveStringRedisTemplate, protobufRedisTemplate,
                saveHistoricalGridScript, props);
    }

    @Bean
    public TelemetryQueryRepository telemetryQueryRepository(RedisTemplate<String, byte[]> protobufRedisTemplate,
                                                             TelemetryQueryArchive telemetryQueryArchive,
                                                             RedisScript<List<byte[]>> queryHistoricalGridScript,
                                                             TelemetryPersistenceRepository telemetryPersistenceRepository) {
        return new TelemetryQueryRepositoryAdapter(telemetryPersistenceRepository, protobufRedisTemplate, queryHistoricalGridScript,
                telemetryQueryArchive, props);
    }

    @Bean
    public TelemetryQueryArchive telemetryQueryArchive(HistoryServiceGrpc.HistoryServiceBlockingStub historyServiceStub) {
        return new TelemetryQueryGrpcAdapter(historyServiceStub, props);
    }

    @Bean
    public TelemetryQueryService telemetryQueryService(TelemetryQueryRepository telemetryQueryRepository) {
        return new TelemetryStateQueryResolver(telemetryQueryRepository);
    }
}