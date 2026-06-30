package me.neobliz1.ecomonitoring.platform.ingestion.config;

import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import me.neobliz1.ecomonitoring.platform.ingestion.controller.ReactiveValidationWebExceptionHandler;
import me.neobliz1.ecomonitoring.platform.ingestion.service.TelemetryIngestionService;
import me.neobliz1.ecomonitoring.platform.ingestion.service.impl.TelemetryIngestionServiceImpl;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import vector.VectorGrpc;

import java.util.concurrent.TimeUnit;

@Configuration
public class IngestionEngineConfiguration {

    @Value("${vector.sidecar.host}")
    private String host;
    @Value("${vector.sidecar.port}")
    private int port;

    @Bean
    public TelemetryIngestionService telemetryIngestionService(VectorGrpc.VectorStub reactiveStub,
                                                               VectorGrpc.VectorBlockingStub blockingStub) {
        return new TelemetryIngestionServiceImpl(reactiveStub, blockingStub);
    }

    @Bean(destroyMethod = "shutdown")
    public ManagedChannel vectorManagedChannel() {
        return NettyChannelBuilder.forAddress(host, port)
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
}
