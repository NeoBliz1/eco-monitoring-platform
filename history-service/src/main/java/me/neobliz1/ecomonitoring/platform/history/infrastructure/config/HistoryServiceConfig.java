package me.neobliz1.ecomonitoring.platform.history.infrastructure.config;

import static me.neobliz1.ecomonitoring.platform.common.constant.PlatformConstants.TX_CHAIN_CONFIRMATION_PROFILE;
import static me.neobliz1.ecomonitoring.platform.common.util.PlatformCommonUtils.resolveSchemaRegistryServer;

import com.zaxxer.hikari.HikariDataSource;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import me.neobliz1.ecomonitoring.platform.common.util.PlatformCommonUtils;
import me.neobliz1.ecomonitoring.platform.history.domain.port.inbound.HistoricalDataConvertService;
import me.neobliz1.ecomonitoring.platform.history.domain.port.outbound.HistoricalPersistenceRepository;
import me.neobliz1.ecomonitoring.platform.history.domain.port.outbound.HistoricalQueryRepository;
import me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter.inbound.grpc.HistoricalExternalCommunicationObserver;
import me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter.inbound.kafka.HistoricalTelemetryListener;
import me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter.outbound.persistence.postgres.HistoricalPersistenceRepositoryAdapter;
import me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter.outbound.persistence.postgres.HistoricalQueryRepositoryAdapter;
import me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter.outbound.persistence.postgres.HistoricalTxIdRepositoryAdapter;
import me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter.outbound.persistence.postgres.HistoricalWeatherGridCellJpaRepository;
import me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter.outbound.persistence.postgres.HistoricalWeatherMapJpaRepository;
import me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter.outbound.persistence.postgres.HistoricalWeatherTelemetryTxIdsJpaRepository;
import me.neobliz1.ecomonitoring.platform.history.infrastructure.mapper.WeatherMapConverter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.jdbc.autoconfigure.DataSourceProperties;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.http.converter.protobuf.ProtobufHttpMessageConverter;

import javax.sql.DataSource;
import java.time.Duration;
import java.util.List;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class HistoryServiceConfig {

    private final DiscoveryClient discoveryClient;
    private final KafkaProperties kafkaProperties;
    private final ConfigurableEnvironment environment;

    @Value("${spring.kafka.service-name}")
    private String kafkaServiceName;
    @Value("${spring.datasource.service-name}")
    private String dataSourceServiceName;
    @Value("${spring.datasource.database}")
    private String dbName;
    @Value("${spring.datasource.schema-name}")
    private String dbSchemaName;
    @Value("${spring.datasource.data-pool.name}")
    private String poolName;
    @Value("${spring.datasource.data-pool.max-pool-size}")
    private int maxPoolSize;
    @Value("${spring.datasource.data-pool.min-idle}")
    private int minIdle;
    @Value("${spring.datasource.data-pool.idle-timeout}")
    private int idleTimeout;
    @Value("${spring.datasource.data-pool.connection-timeout}")
    private int connectionTimeout;

    @Bean
    public HistoricalQueryRepository historicalQueryRepository(HistoricalWeatherMapJpaRepository weatherMapJpaRepository,
                                                               HistoricalWeatherGridCellJpaRepository gridCellJpaRepository) {
        return new HistoricalQueryRepositoryAdapter(weatherMapJpaRepository, gridCellJpaRepository);
    }

    @Bean
    public HistoricalDataConvertService weatherMapConverter() {
        return new WeatherMapConverter();
    }

    @Bean
    @Profile(TX_CHAIN_CONFIRMATION_PROFILE)
    public HistoricalTxIdRepositoryAdapter historicalTxIdRepositoryAdapter(HistoricalWeatherTelemetryTxIdsJpaRepository jpaTxIdsRepository) {
        return new HistoricalTxIdRepositoryAdapter(jpaTxIdsRepository);
    }

    @Bean
    public HistoricalPersistenceRepository historicalPersistenceRepository(HistoricalDataConvertService weatherMapConverter,
                                                                           HistoricalWeatherMapJpaRepository weatherMapJpaRepository,
                                                                           HistoricalTxIdRepositoryAdapter txIdAdapter) {
        return new HistoricalPersistenceRepositoryAdapter(weatherMapConverter, weatherMapJpaRepository, txIdAdapter);
    }

    @Bean
    public HistoricalExternalCommunicationObserver historicalExternalCommunicationObserver(HistoricalQueryRepository queryRepositoryAdapter,
                                                                                           HistoricalDataConvertService weatherMapConverter) {
        return new HistoricalExternalCommunicationObserver(queryRepositoryAdapter, weatherMapConverter);
    }

    @Bean
    public HistoricalTelemetryListener historicalTelemetryListener(HistoricalPersistenceRepository historicalPersistenceRepository) {
        return new HistoricalTelemetryListener(historicalPersistenceRepository);
    }

    @Bean
    @Primary
    public DataSource dataSource(DataSourceProperties properties) {
        PlatformCommonUtils.ServiceAddressRecord serviceAddress = PlatformCommonUtils.discoverServiceAddressFromConsulServerByName(discoveryClient,
                environment, dataSourceServiceName);
        String jdbcUrl = String.format("jdbc:postgresql://%s:%d/%s?currentSchema=%s", serviceAddress.resolvedHost(), serviceAddress.resolvedPort(),
                dbName, dbSchemaName);
        if(log.isDebugEnabled()) {
            log.debug("Resolved JDBC URL from Consul: {}", jdbcUrl);
        }
        properties.setUrl(jdbcUrl);
        HikariDataSource hikariDataSource = properties.initializeDataSourceBuilder()
                .type(HikariDataSource.class)
                .build();
        hikariDataSource.setPoolName(poolName);
        hikariDataSource.setMaximumPoolSize(maxPoolSize);
        hikariDataSource.setMinimumIdle(minIdle);
        hikariDataSource.setIdleTimeout(Duration.ofSeconds(idleTimeout).toMillis());
        hikariDataSource.setConnectionTimeout(Duration.ofSeconds(connectionTimeout).toMillis());
        log.info("HikariCP pool initialized: {}", jdbcUrl);
        return hikariDataSource;
    }

    @Bean
    public ProtobufHttpMessageConverter protobufHttpMessageConverter() {
        return new ProtobufHttpMessageConverter();
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
        if(log.isDebugEnabled()) {
            log.debug("Kafka bootstrap servers resolved via Consul: {}", serviceAddress);
        }
    }
}