package me.neobliz1.ecomonitoring.platform.ingestion.infrastructure.config;

import static me.neobliz1.ecomonitoring.platform.common.util.PlatformCommonUtils.resolveSchemaRegistryServer;

import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import jakarta.annotation.PostConstruct;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import me.neobliz1.ecomonitoring.platform.common.util.PlatformCommonUtils;
import me.neobliz1.ecomonitoring.platform.common.util.PlatformCommonUtils.ServiceAddressRecord;
import me.neobliz1.ecomonitoring.platform.ingestion.domain.port.inbound.TelemetryIngestionService;
import me.neobliz1.ecomonitoring.platform.ingestion.domain.service.ReactiveValidationWebExceptionHandler;
import me.neobliz1.ecomonitoring.platform.ingestion.infrastructure.adapter.outbound.grpc.vector.TelemetryIngestionAdapter;
import me.neobliz1.ecomonitoring.platform.ingestion.infrastructure.adapter.outbound.grpc.vector.TelemetryPayloadMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.core.env.ConfigurableEnvironment;
import vector.VectorGrpc;

import java.util.concurrent.TimeUnit;

@Configuration
@RequiredArgsConstructor
public class IngestionServiceConfiguration {

    private final DiscoveryClient discoveryClient;
    private final ConfigurableEnvironment environment;

    @Value("${VECTOR_SERVICE_NAME}")
    private String vectorServiceName;

    @Bean
    public TelemetryIngestionService telemetryIngestionService(VectorGrpc.VectorStub reactiveStub,
                                                               VectorGrpc.VectorBlockingStub blockingStub,
                                                               TelemetryPayloadMapper telemetryPayloadMapper) {
        return new TelemetryIngestionAdapter(reactiveStub, blockingStub, telemetryPayloadMapper);
    }

    @Bean
    public TelemetryPayloadMapper vectorPayloadMapper() {
        return new TelemetryPayloadMapper();
    }

    @Bean(destroyMethod = "shutdown")
    public ManagedChannel vectorManagedChannel(@NonNull ConfigurableEnvironment environment) {
        ServiceAddressRecord serviceAddress = PlatformCommonUtils.discoverServiceAddressFromConsulServerByName(discoveryClient,
                environment, vectorServiceName);
        return NettyChannelBuilder.forAddress(serviceAddress.resolvedHost(), serviceAddress.resolvedPort())
                .usePlaintext()
                .keepAliveTime(30, TimeUnit.SECONDS)
                .build();
    }

    @Bean
    public VectorGrpc.VectorStub vectorReactiveStub(ManagedChannel channel) {
        return VectorGrpc.newStub(channel);
    }

    @Bean
    public VectorGrpc.VectorBlockingStub vectorStandardBlockingStub(ManagedChannel channel) {
        return VectorGrpc.newBlockingStub(channel);
    }

    @Bean
    @Order(-2)
    public ReactiveValidationWebExceptionHandler reactiveValidationWebExceptionHandler() {
        return new ReactiveValidationWebExceptionHandler();
    }

    @PostConstruct
    public void resolveEnvironmentBootstrapServers() {
        resolveSchemaRegistryServer(discoveryClient, environment);
    }
}
