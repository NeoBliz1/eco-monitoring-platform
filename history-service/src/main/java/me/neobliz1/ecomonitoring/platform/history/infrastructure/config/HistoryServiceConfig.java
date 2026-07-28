package me.neobliz1.ecomonitoring.platform.history.infrastructure.config;

import static me.neobliz1.ecomonitoring.platform.common.util.PlatformCommonUtils.resolveSchemaRegistryServer;

import com.zaxxer.hikari.HikariDataSource;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import me.neobliz1.ecomonitoring.platform.common.util.PlatformCommonUtils;
import me.neobliz1.ecomonitoring.platform.history.domain.inbound.HistoricalService;
import me.neobliz1.ecomonitoring.platform.history.domain.outbound.HistoricalPersistenceRepository;
import me.neobliz1.ecomonitoring.platform.history.domain.outbound.HistoricalQueryRepository;
import me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter.inbound.kafka.HistoricalTelemetryListener;
import me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter.outbound.persistence.postgres.HistoricalQueryJpaRepository;
import me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter.outbound.persistence.postgres.HistoricalQueryRepositoryAdapter;
import me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter.outbound.persistence.postgres.TelemetryHistoryPersistenceRepositoryAdapter;
import me.neobliz1.ecomonitoring.platform.history.infrastructure.mapper.WeatherMapConverter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.jdbc.autoconfigure.DataSourceProperties;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.env.ConfigurableEnvironment;

import javax.sql.DataSource;
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

    @Bean
    public HistoricalQueryRepository historicalQueryRepository(HistoricalQueryJpaRepository jpaRepository) {
        return new HistoricalQueryRepositoryAdapter(jpaRepository);
    }

    @Bean
    public HistoricalService weatherMapConverter() {
        return new WeatherMapConverter();
    }

    @Bean
    public HistoricalPersistenceRepository historicalPersistenceRepository(HistoricalQueryRepository queryRepositoryAdapter,
                                                                           HistoricalService weatherMapConverter,
                                                                           HistoricalQueryJpaRepository jpaRepository) {
        return new TelemetryHistoryPersistenceRepositoryAdapter(queryRepositoryAdapter, weatherMapConverter, jpaRepository);
    }

    @Bean
    public HistoricalTelemetryListener historicalTelemetryListener(HistoricalPersistenceRepository historicalPersistenceRepository) {
        return new HistoricalTelemetryListener(historicalPersistenceRepository);
    }

    @Bean
    @Primary
    public DataSource dataSource(DataSourceProperties properties) {
        log.debug("📡 Resolving datasource matrix location coordinates via Consul Discovery client...");
        PlatformCommonUtils.ServiceAddressRecord serviceAddress = PlatformCommonUtils.discoverServiceAddressFromConsulServerByName(discoveryClient,
                environment, dataSourceServiceName);
        String computedJdbcUrl = String.format("jdbc:postgresql://%s:%d/%s?currentSchema=%s", serviceAddress.resolvedHost(), serviceAddress.resolvedPort(),
                dbName, dbSchemaName);
        log.debug("✅ Dynamically generated target JDBC connection path: {}", computedJdbcUrl);
        properties.setUrl(computedJdbcUrl);
        HikariDataSource hikariDataSource = properties.initializeDataSourceBuilder()
                .type(HikariDataSource.class)
                .build();
        hikariDataSource.setPoolName("EcoHistoryTelemetry-HikariPool");
        hikariDataSource.setMaximumPoolSize(20);
        hikariDataSource.setMinimumIdle(5);
        hikariDataSource.setIdleTimeout(30000);
        hikariDataSource.setConnectionTimeout(10000);
        log.info("✅ HikariCP Concurrency Connection Pool instantiated successfully targeting: {}", computedJdbcUrl);
        return hikariDataSource;
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