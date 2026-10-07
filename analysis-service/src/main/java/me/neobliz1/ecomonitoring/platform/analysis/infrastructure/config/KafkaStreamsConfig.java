package me.neobliz1.ecomonitoring.platform.analysis.infrastructure.config;

import static me.neobliz1.ecomonitoring.platform.common.util.PlatformCommonUtils.resolveSchemaRegistryServer;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import me.neobliz1.ecomonitoring.platform.analysis.domain.port.inbound.TelemetryAnalysisService;
import me.neobliz1.ecomonitoring.platform.analysis.domain.port.outbound.TelemetryPersistentService;
import me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.messaging.kafka.TelemetryStreamsTopologyOrchestrator;
import me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.messaging.kafka.processor.WeatherPacketStreamAggregationProcessor;
import me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.messaging.kafka.processor.WeatherPacketStreamDeduplicationProcessor;
import me.neobliz1.ecomonitoring.platform.common.util.PlatformCommonUtils;
import me.neobliz1.ecomonitoring.platform.model.record.ServiceAddressRecord;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.WeatherPacket;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;
import org.springframework.grpc.client.ImportGrpcClients;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import weather.history.HistoryServiceGrpc;

import java.util.List;
import java.util.Map;

@Slf4j
@Configuration
@EnableKafkaStreams
@RequiredArgsConstructor
@ImportGrpcClients(target = "history-service", types = HistoryServiceGrpc.HistoryServiceBlockingStub.class)
public class KafkaStreamsConfig {

    public static final String SCOPE_PROTOTYPE_NAME = "prototype";

    private final DiscoveryClient discoveryClient;
    private final KafkaProperties kafkaProperties;
    private final ConfigurableEnvironment environment;
    private final AnalysisInfrastructureProperties props;


    @Bean
    @Scope(SCOPE_PROTOTYPE_NAME)
    public WeatherPacketStreamDeduplicationProcessor telemetryDeduplicationProcessor() {
        return new WeatherPacketStreamDeduplicationProcessor(props);
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

    @Bean(name = "kafkaStream")
    public KStream<String, WeatherPacket> topologyOrchestratorStream(TelemetryAnalysisService telemetryAnalysisService,
                                                                     StreamsBuilder streamsBuilder) {
        return telemetryAnalysisService.buildTopology(streamsBuilder);
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