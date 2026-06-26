package me.neobliz1.ecomonitoring.platform.ingestion.config;

import static me.neobliz1.ecomonitoring.platform.shared.contracts.proto.ReactorVectorIngestServiceGrpc.newReactorStub;
import static me.neobliz1.ecomonitoring.platform.shared.contracts.proto.VectorIngestServiceGrpc.newStub;

import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import me.neobliz1.ecomonitoring.platform.ingestion.controller.ReactiveValidationWebExceptionHandler;
import me.neobliz1.ecomonitoring.platform.ingestion.service.TelemetryIngestionService;
import me.neobliz1.ecomonitoring.platform.ingestion.service.impl.TelemetryIngestionServiceImpl;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.ReactorVectorIngestServiceGrpc;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.VectorIngestServiceGrpc.VectorIngestServiceStub;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;

import java.util.concurrent.TimeUnit;

@Configuration
public class IngestionEngineConfiguration {

    @Value("${vector.sidecar.host}")
    private String host;
    @Value("${vector.sidecar.port}")
    private int port;

    @Bean
    public TelemetryIngestionService telemetryIngestionService(ManagedChannel channel) {
        return new TelemetryIngestionServiceImpl(channel);
    }

    @Bean(destroyMethod = "shutdown")
    public ManagedChannel vectorManagedChannel() {
        return NettyChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .keepAliveTime(30, TimeUnit.SECONDS)
                .build();
    }

    @Bean
    public ReactorVectorIngestServiceGrpc.ReactorVectorIngestServiceStub vectorReactiveStub(ManagedChannel channel) {
        return newReactorStub(channel);
    }

    @Bean
    public VectorIngestServiceStub vectorStandardAsyncStub(ManagedChannel channel) {
        return newStub(channel);
    }

    @Bean
    @Order(-2) // Executes before Spring Boot's default error page handler (-1)
    public ReactiveValidationWebExceptionHandler reactiveValidationWebExceptionHandler() {
        return new ReactiveValidationWebExceptionHandler();
    }
}
