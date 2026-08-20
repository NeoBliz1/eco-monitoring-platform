package me.neobliz1.ecomonitoring.platform.analysis.infrastructure.config;

import static me.neobliz1.ecomonitoring.platform.common.util.PlatformCommonUtils.resolveSchemaRegistryServer;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import me.neobliz1.ecomonitoring.platform.analysis.domain.port.inbound.TelemetryAnalysisService;
import me.neobliz1.ecomonitoring.platform.analysis.domain.port.inbound.TelemetryQueryService;
import me.neobliz1.ecomonitoring.platform.analysis.domain.port.outbound.TelemetryPersistenceRepository;
import me.neobliz1.ecomonitoring.platform.analysis.domain.port.outbound.TelemetryPersistentService;
import me.neobliz1.ecomonitoring.platform.analysis.domain.port.outbound.TelemetryQueryRepository;
import me.neobliz1.ecomonitoring.platform.analysis.domain.service.TelemetryStatePersister;
import me.neobliz1.ecomonitoring.platform.analysis.domain.service.TelemetryStateQueryResolver;
import me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.messaging.kafka.TelemetryTopologyOrchestrator;
import me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.persistence.redis.TelemetryPersistenceRepositoryAdapter;
import me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.persistence.redis.TelemetryQueryRepositoryAdapter;
import me.neobliz1.ecomonitoring.platform.common.util.PlatformCommonUtils;
import me.neobliz1.ecomonitoring.platform.model.exception.RedisPasswordNotSetException;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.WeatherPacket;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.connection.RedisPassword;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.scripting.support.ResourceScriptSource;

import java.time.Duration;
import java.util.List;

@Slf4j
@Configuration
@EnableKafkaStreams
@RequiredArgsConstructor
public class AnalysisServiceConfig {

    private final DiscoveryClient discoveryClient;
    private final KafkaProperties kafkaProperties;
    private final ConfigurableEnvironment environment;

    @Value("${spring.data.redis.service-name}")
    private String redisServiceName;
    @Value("${spring.data.redis.password:}")
    private String redisPassword;
    @Value("${spring.kafka.service-name}")
    private String kafkaServiceName;

    @Bean
    public TelemetryPersistentService telemetryPersistentService(TelemetryPersistenceRepository telemetryRepository) {
        return new TelemetryStatePersister(telemetryRepository);
    }

    @Bean
    public TelemetryPersistenceRepository telemetryPersistenceRepository(ReactiveStringRedisTemplate reactiveStringRedisTemplate,
                                                                         RedisTemplate<String, byte[]> protobufRedisTemplate,
                                                                         DefaultRedisScript<String> saveHistoricalGridScript) {
        return new TelemetryPersistenceRepositoryAdapter(reactiveStringRedisTemplate, protobufRedisTemplate, saveHistoricalGridScript);
    }

    @Bean
    public TelemetryQueryRepository telemetryQueryRepository(RedisTemplate<String, byte[]> protobufRedisTemplate) {
        return new TelemetryQueryRepositoryAdapter(protobufRedisTemplate);
    }

    @Bean
    public TelemetryQueryService telemetryQueryService(TelemetryQueryRepository telemetryQueryRepository) {
        return new TelemetryStateQueryResolver(telemetryQueryRepository);
    }

    @Bean
    public TelemetryAnalysisService telemetryAnalysisService(TelemetryPersistentService persistentService) {
        return new TelemetryTopologyOrchestrator(persistentService);
    }

    @Bean(name = "kafkaStream")
    public KStream<String, WeatherPacket> preventWeatherPacketDuplicationStream(TelemetryAnalysisService telemetryAnalysisService,
                                                                                StreamsBuilder streamsBuilder) {
        return telemetryAnalysisService.buildTopology(streamsBuilder);
    }

    @Bean
    public LettuceConnectionFactory redisConnectionFactory(DiscoveryClient discoveryClient) {
        PlatformCommonUtils.ServiceAddressRecord serviceAddress = PlatformCommonUtils.discoverServiceAddressFromConsulServerByName(
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
    public DefaultRedisScript<String> saveHistoricalGridScript() {
        DefaultRedisScript<String> script = new DefaultRedisScript<>();
        script.setScriptSource(new ResourceScriptSource(new ClassPathResource("lua/scripts/save_historical_grid.lua")));
        script.setResultType(String.class);
        return script;
    }

    @Bean
    public ReactiveStringRedisTemplate reactiveStringRedisTemplate(ReactiveRedisConnectionFactory factory) {
        return new ReactiveStringRedisTemplate(factory);
    }

    @PostConstruct
    public void resolveEnvironmentBootstrapServers() {
        resolveKafkaBootstrapServers();
        resolveSchemaRegistryServer(discoveryClient, environment);
    }

    private void resolveKafkaBootstrapServers() {
        List<String> serviceAddress = PlatformCommonUtils.discoverServiceAddressesFromConsulServerByName(discoveryClient,
                environment, kafkaServiceName);
        kafkaProperties.setBootstrapServers(serviceAddress);
        log.info("Consul dynamically routed Kafka to: {}", serviceAddress);
    }
}