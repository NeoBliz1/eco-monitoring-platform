package me.neobliz1.ecomonitoring.platform.analysis.config;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import me.neobliz1.ecomonitoring.platform.analysis.service.TelemetryAnalysisService;
import me.neobliz1.ecomonitoring.platform.analysis.service.impl.TelemetryAnalysisServiceImpl;
import me.neobliz1.ecomonitoring.platform.common.util.PlatformCommonUtils;
import me.neobliz1.ecomonitoring.platform.common.util.PlatformCommonUtils.ServiceAddressRecord;
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
import org.springframework.core.env.MapPropertySource;
import org.springframework.data.redis.connection.RedisPassword;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.annotation.EnableKafkaStreams;

import java.util.List;
import java.util.Map;

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
    public TelemetryAnalysisService telemetryAnalysisService(StringRedisTemplate redisTemplate) {
        return new TelemetryAnalysisServiceImpl(redisTemplate);
    }

    @Bean(name = "kafkaStream")
    public KStream<String, WeatherPacket> preventWeatherPacketDuplicationStream(TelemetryAnalysisService telemetryAnalysisService,
                                                                                StreamsBuilder streamsBuilder) {
        return telemetryAnalysisService.buildTopology(streamsBuilder);
    }

    @Bean
    public LettuceConnectionFactory redisConnectionFactory() {
        ServiceAddressRecord serviceAddress = PlatformCommonUtils.discoverServiceAddressFromConsulServerByName(discoveryClient,
                environment, redisServiceName);

        RedisStandaloneConfiguration config = new RedisStandaloneConfiguration();
        config.setHostName(serviceAddress.resolvedHost());
        config.setPort(serviceAddress.resolvedPort());

        if(redisPassword!=null && !redisPassword.isBlank()) {
            config.setPassword(RedisPassword.of(redisPassword));
        } else {
            throw new RedisPasswordNotSetException();
        }

        return new LettuceConnectionFactory(config);
    }

    @PostConstruct
    public void resolveEnvironmentBootstrapServers() {
        resolveKafkaBootstrapServers();
        resolveSchemaRegistryServer();
    }

    private void resolveKafkaBootstrapServers() {
        List<String> serviceAddress = PlatformCommonUtils.discoverServiceAddressesFromConsulServerByName(discoveryClient,
                environment, kafkaServiceName);
        kafkaProperties.setBootstrapServers(serviceAddress);
        log.info("Consul dynamically routed Kafka to: {}", serviceAddress);
    }

    private void resolveSchemaRegistryServer() {
        ServiceAddressRecord registryRecord = PlatformCommonUtils.discoverServiceAddressFromConsulServerByName(
                discoveryClient, environment, "schema-registry");
        String schemaRegistryUrl = String.format("http://%s:%s", registryRecord.resolvedHost(), registryRecord.resolvedPort());
        environment.getPropertySources().addFirst(
                new MapPropertySource("consulDynamicSchemaRegistryProps",
                        Map.of("spring.kafka.streams.properties.schema.registry.url", schemaRegistryUrl))
        );
        log.info("Consul dynamically routed Schema registry to: {}", schemaRegistryUrl);
    }
}