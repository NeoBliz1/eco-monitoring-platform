package me.neobliz1.ecomonitoring.platform.analysis.infrastructure.config;

import static me.neobliz1.ecomonitoring.platform.common.util.PlatformCommonUtils.resolveSchemaRegistryServer;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import me.neobliz1.ecomonitoring.platform.analysis.domain.port.inbound.TelemetryAnalysisService;
import me.neobliz1.ecomonitoring.platform.analysis.domain.port.inbound.TelemetryQueryService;
import me.neobliz1.ecomonitoring.platform.analysis.domain.port.outbound.TelemetryPersistenceRepository;
import me.neobliz1.ecomonitoring.platform.analysis.domain.port.outbound.TelemetryPersistentService;
import me.neobliz1.ecomonitoring.platform.analysis.domain.port.outbound.TelemetryQueryArchive;
import me.neobliz1.ecomonitoring.platform.analysis.domain.port.outbound.TelemetryQueryRepository;
import me.neobliz1.ecomonitoring.platform.analysis.domain.service.TelemetryStatePersister;
import me.neobliz1.ecomonitoring.platform.analysis.domain.service.TelemetryStateQueryResolver;
import me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.history.grpc.TelemetryQueryGrpcAdapter;
import me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.messaging.kafka.TelemetryStreamsTopologyOrchestrator;
import me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.messaging.kafka.processor.WeatherPacketStreamAggregationProcessor;
import me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.messaging.kafka.processor.WeatherPacketStreamDeduplicationProcessor;
import me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.persistence.redis.TelemetryPersistenceRepositoryAdapter;
import me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.persistence.redis.TelemetryQueryRepositoryAdapter;
import me.neobliz1.ecomonitoring.platform.common.util.PlatformCommonUtils;
import me.neobliz1.ecomonitoring.platform.model.exception.RedisPasswordNotSetException;
import me.neobliz1.ecomonitoring.platform.model.record.ServiceAddressRecord;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.WeatherPacket;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.connection.RedisPassword;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.grpc.client.ImportGrpcClients;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import weather.history.HistoryServiceGrpc;

import java.time.Duration;
import java.util.List;
import java.util.Map;

@Slf4j
@Configuration
@EnableKafkaStreams
@RequiredArgsConstructor
@ImportGrpcClients(target = "history-service", types = HistoryServiceGrpc.HistoryServiceBlockingStub.class)
public class AnalysisServiceConfig {

    public static final String SCOPE_PROTOTYPE_NAME = "prototype";

    private final DiscoveryClient discoveryClient;
    private final KafkaProperties kafkaProperties;
    private final ConfigurableEnvironment environment;
    private final AnalysisInfrastructureProperties props;

    @Bean
    public TelemetryPersistentService telemetryPersistentService(TelemetryPersistenceRepository telemetryRepository) {
        return new TelemetryStatePersister(telemetryRepository, props);
    }

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
    @Scope(SCOPE_PROTOTYPE_NAME)
    public WeatherPacketStreamDeduplicationProcessor telemetryDeduplicationProcessor(
            @Value("${spring.kafka.streams.pipeline.name.deduplication-processor.interval}") Long deduplicationInterval) {
        return new WeatherPacketStreamDeduplicationProcessor(deduplicationInterval);
    }

    @Bean
    @Scope(SCOPE_PROTOTYPE_NAME)
    public WeatherPacketStreamAggregationProcessor telemetryAggregationProcessor(TelemetryPersistentService persistentService) {
        return new WeatherPacketStreamAggregationProcessor(props, persistentService);
    }

    @Bean
    public TelemetryAnalysisService telemetryAnalysisService(ObjectProvider<WeatherPacketStreamDeduplicationProcessor> dedupProvider,
                                                             ObjectProvider<WeatherPacketStreamAggregationProcessor> aggProvider) {
        return new TelemetryStreamsTopologyOrchestrator(dedupProvider, aggProvider, props);
    }

    @Bean
    public TelemetryQueryService telemetryQueryService(TelemetryQueryRepository telemetryQueryRepository) {
        return new TelemetryStateQueryResolver(telemetryQueryRepository);
    }

    @Bean(name = "kafkaStream")
    public KStream<String, WeatherPacket> topologyOrchestratorStream(TelemetryAnalysisService telemetryAnalysisService,
                                                                     StreamsBuilder streamsBuilder) {
        return telemetryAnalysisService.buildTopology(streamsBuilder);
    }

    @Bean
    public LettuceConnectionFactory redisConnectionFactory(DiscoveryClient discoveryClient,
                                                           @Value("${spring.data.redis.password:}") String redisPassword,
                                                           @Value("${spring.data.redis.service-name}") String redisServiceName) {
        ServiceAddressRecord serviceAddress = PlatformCommonUtils.discoverServiceAddressFromConsulServerByName(
                discoveryClient, environment, redisServiceName);

        RedisStandaloneConfiguration config = new RedisStandaloneConfiguration();
        config.setHostName(serviceAddress.resolvedHost());
        config.setPort(serviceAddress.resolvedPort());

        if(redisPassword!=null && !redisPassword.isBlank()) {
            config.setPassword(RedisPassword.of(redisPassword));
        } else {
            throw new RedisPasswordNotSetException();
        }

        LettuceClientConfiguration clientConfig = LettuceClientConfiguration.builder()
                .commandTimeout(Duration.ofMillis(200))
                .shutdownTimeout(Duration.ofMillis(100))
                .build();

        return new LettuceConnectionFactory(config, clientConfig);
    }

    @Bean
    public RedisTemplate<String, byte[]> protobufRedisTemplate(LettuceConnectionFactory connectionFactory) {
        RedisTemplate<String, byte[]> template = new RedisTemplate<>();
        template.setConnectionFactory(connectionFactory);
        template.setKeySerializer(RedisSerializer.string());
        template.setValueSerializer(RedisSerializer.byteArray());
        template.setHashKeySerializer(RedisSerializer.string());
        template.setHashValueSerializer(RedisSerializer.byteArray());
        return template;
    }

    @Bean
    public ReactiveStringRedisTemplate reactiveStringRedisTemplate(ReactiveRedisConnectionFactory factory) {
        return new ReactiveStringRedisTemplate(factory);
    }

    @PostConstruct
    public void resolveEnvironmentBootstrapServers() {
        resolveKafkaBootstrapServers();
        resolveSchemaRegistryServer(discoveryClient, environment);
        resolveHistoryGrpcServer();
    }

    private void resolveKafkaBootstrapServers() {
        String kafkaServiceName = props.getKafka().getServiceName();
        List<String> serviceAddress = PlatformCommonUtils.discoverServiceAddressesFromConsulServerByName(discoveryClient,
                environment, kafkaServiceName);
        kafkaProperties.setBootstrapServers(serviceAddress);
        log.info("Kafka bootstrap servers: {}", serviceAddress);
    }

    private void resolveHistoryGrpcServer() {
        val historyService = props.getGrpc().getClient().getChannel().getHistoryService();
        ServiceAddressRecord registryRecord = PlatformCommonUtils.discoverServiceAddressFromConsulServerByName(
                discoveryClient, environment, historyService.getServiceName());
        String staticGrpcTargetUri = "static://"+registryRecord.resolvedHost()+":"+historyService.getServicePort();
        Map<String, Object> grpcDynamicProperties = Map.of("spring.grpc.client.channel.history-service.target", staticGrpcTargetUri);
        MapPropertySource dynamicSource = new MapPropertySource("grpcConsulDynamicOverrides", grpcDynamicProperties);
        environment.getPropertySources().addFirst(dynamicSource);
        log.info("Successfully bound history-service gRPC client route to target configuration: {}", staticGrpcTargetUri);
    }
}