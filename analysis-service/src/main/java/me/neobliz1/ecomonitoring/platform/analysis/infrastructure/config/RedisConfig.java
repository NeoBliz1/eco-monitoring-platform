package me.neobliz1.ecomonitoring.platform.analysis.infrastructure.config;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import me.neobliz1.ecomonitoring.platform.common.util.PlatformCommonUtils;
import me.neobliz1.ecomonitoring.platform.model.exception.RedisPasswordNotSetException;
import me.neobliz1.ecomonitoring.platform.model.record.ServiceAddressRecord;
import org.springframework.beans.factory.annotation.Value;
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
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.grpc.client.ImportGrpcClients;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import weather.history.HistoryServiceGrpc;

import java.time.Duration;
import java.util.List;

@Slf4j
@Configuration
@EnableKafkaStreams
@RequiredArgsConstructor
@ImportGrpcClients(target = "history-service", types = HistoryServiceGrpc.HistoryServiceBlockingStub.class)
public class RedisConfig {

    private final ConfigurableEnvironment environment;

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
    public LettuceConnectionFactory redisConnectionFactory(DiscoveryClient discoveryClient,
                                                           @Value("${spring.data.redis.password:}") String redisPassword,
                                                           @Value("${spring.data.redis.service-name}") String redisServiceName) {
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
}