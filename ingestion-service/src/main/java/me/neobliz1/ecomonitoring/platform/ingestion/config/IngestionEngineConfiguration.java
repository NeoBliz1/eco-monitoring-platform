package me.neobliz1.ecomonitoring.platform.ingestion.config;

import io.grpc.ManagedChannel;
import me.neobliz1.ecomonitoring.platform.ingestion.service.TelemetryIngestionService;
import me.neobliz1.ecomonitoring.platform.ingestion.service.impl.TelemetryIngestionServiceImpl;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class IngestionEngineConfiguration {

    @Bean
    public TelemetryIngestionService telemetryIngestionService(ManagedChannel channel) {
        return new TelemetryIngestionServiceImpl(channel);
    }
}
