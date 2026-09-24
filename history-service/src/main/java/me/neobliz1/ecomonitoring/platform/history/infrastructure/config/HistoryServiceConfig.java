package me.neobliz1.ecomonitoring.platform.history.infrastructure.config;

import static me.neobliz1.ecomonitoring.platform.common.constant.PlatformConstants.SPRING_SCHEMA_REGISTRY_URL_PROP_NAME;
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
import me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter.outbound.persistence.postgres.jpa.HistoricalWeatherGridCellJpaRepository;
import me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter.outbound.persistence.postgres.jpa.HistoricalWeatherMapJpaRepository;
import me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter.outbound.persistence.postgres.jpa.HistoricalWeatherTelemetryDltJpaRepository;
import me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter.outbound.persistence.postgres.jpa.HistoricalWeatherTelemetryTxIdsJpaRepository;
import me.neobliz1.ecomonitoring.platform.history.infrastructure.mapper.WeatherMapConverter;
import me.neobliz1.ecomonitoring.platform.model.record.ServiceAddressRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.jdbc.autoconfigure.DataSourceProperties;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.cache.CacheManager;
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
    private final HistoryInfrastructureProperties infraProps;

    @Bean
    public HistoricalQueryRepository historicalQueryRepository(HistoricalWeatherMapJpaRepository weatherMapJpaRepository,
                                                               HistoricalWeatherGridCellJpaRepository gridCellJpaRepository) {
        return new HistoricalQueryRepositoryAdapter(weatherMapJpaRepository, gridCellJpaRepository);
    }

    @Bean
    public HistoricalDataConvertService weatherMapConverter(HistoricalWeatherGridCellJpaRepository gridCellJpaRepository) {
        return new WeatherMapConverter(gridCellJpaRepository);
    }

    @Bean
    @Profile(TX_CHAIN_CONFIRMATION_PROFILE)
    public HistoricalTxIdRepositoryAdapter historicalTxIdRepositoryAdapter(HistoricalWeatherTelemetryTxIdsJpaRepository jpaTxIdsRepository) {
        return new HistoricalTxIdRepositoryAdapter(jpaTxIdsRepository);
    }

    @Bean
    public HistoricalPersistenceRepository historicalPersistenceRepository(HistoricalWeatherTelemetryDltJpaRepository dltJpaRepository,
                                                                           HistoricalWeatherGridCellJpaRepository gridCellJpaRepository,
                                                                           HistoricalDataConvertService weatherMapConverter,
                                                                           HistoricalWeatherMapJpaRepository weatherMapJpaRepository,
                                                                           @Autowired(required = false) HistoricalTxIdRepositoryAdapter txIdAdapter,
                                                                           CacheManager springL1CacheManager) {
        return new HistoricalPersistenceRepositoryAdapter(dltJpaRepository, gridCellJpaRepository, weatherMapConverter,
                weatherMapJpaRepository, txIdAdapter, springL1CacheManager);
    }

    @Bean
    public HistoricalExternalCommunicationObserver historicalExternalCommunicationObserver(HistoricalQueryRepository queryRepositoryAdapter,
                                                                                           HistoricalDataConvertService weatherMapConverter,
                                                                                           CacheManager springL1CacheManager) {
        return new HistoricalExternalCommunicationObserver(queryRepositoryAdapter, weatherMapConverter, springL1CacheManager);
    }

    @Bean
    public HistoricalTelemetryListener historicalTelemetryListener(HistoricalPersistenceRepository historicalPersistenceRepository) {
        return new HistoricalTelemetryListener(historicalPersistenceRepository);
    }

    @Bean
    @Primary
    public DataSource dataSource(DataSourceProperties properties) {
        HistoryInfrastructureProperties.Datasource datasource = infraProps.getDatasource();
        String dataSourceServiceName = datasource.getServiceName();
        ServiceAddressRecord serviceAddress = PlatformCommonUtils.discoverServiceAddressFromConsulServerByName(discoveryClient,
                environment, dataSourceServiceName);
        String jdbcUrl = String.format("jdbc:postgresql://%s:%d/%s?currentSchema=%s", serviceAddress.resolvedHost(), serviceAddress.resolvedPort(),
                datasource.getDatabase(), datasource.getSchemaName());
        if(log.isDebugEnabled()) {
            log.debug("Resolved JDBC URL from Consul: {}", jdbcUrl);
        }
        properties.setUrl(jdbcUrl);
        HikariDataSource hikariDataSource = properties.initializeDataSourceBuilder()
                .type(HikariDataSource.class)
                .build();
        HistoryInfrastructureProperties.Datasource.DataPool dataPool = datasource.getDataPool();
        hikariDataSource.setPoolName(dataPool.getName());
        hikariDataSource.setMaximumPoolSize(dataPool.getMaxPoolSize());
        hikariDataSource.setMinimumIdle(dataPool.getMinIdle());
        hikariDataSource.setIdleTimeout(Duration.ofSeconds(dataPool.getIdleTimeout()).toMillis());
        hikariDataSource.setConnectionTimeout(Duration.ofSeconds(dataPool.getConnectionTimeout()).toMillis());
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
        String schemaRegistryUrl = environment.getProperty(SPRING_SCHEMA_REGISTRY_URL_PROP_NAME);
        infraProps.getKafka().getStreams().getProperties().getSchema().getRegistry().setUrl(schemaRegistryUrl);
    }

    private void resolveKafkaBootstrapServers() {
        String kafkaServiceName = environment.getProperty("spring.kafka.service-name");
        if(kafkaServiceName==null || kafkaServiceName.isBlank()) {
            throw new IllegalStateException("Failed to resolve bootstrap servers: 'spring.kafka.service-name' property is missing or empty.");
        }
        List<String> serviceAddress = PlatformCommonUtils.discoverServiceAddressesFromConsulServerByName(discoveryClient,
                environment, kafkaServiceName);
        kafkaProperties.setBootstrapServers(serviceAddress);
        if(log.isDebugEnabled()) {
            log.debug("Kafka bootstrap servers resolved via Consul: {}", serviceAddress);
        }
    }
}