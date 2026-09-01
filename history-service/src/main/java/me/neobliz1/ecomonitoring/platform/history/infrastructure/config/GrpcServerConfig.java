package me.neobliz1.ecomonitoring.platform.history.infrastructure.config;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter.inbound.grpc.HistoricalExternalCommunicationObserver;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;

@Configuration
public class GrpcServerConfig {

    @Bean(destroyMethod = "shutdown")
    public Server grpcServer(HistoricalExternalCommunicationObserver historyService, @Value("${grpc.server.port}") int grpcPort) throws IOException {
        Server server = ServerBuilder.forPort(grpcPort)
                .addService(historyService)
                .build();
        server.start();
        return server;
    }
}