package me.neobliz1.ecomonitoring.platform.ingestion.infrastructure.config;

import static me.neobliz1.ecomonitoring.platform.common.constant.PlatformConstants.TRACE_PROFILE;

import me.neobliz1.ecomonitoring.platform.ingestion.infrastructure.aspect.TelemetryTracingAspect;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.context.annotation.Profile;

@Configuration
@Profile(TRACE_PROFILE)
@EnableAspectJAutoProxy
public class TelemetryTracingConfig {

    @Bean
    public TelemetryTracingAspect telemetryTracingAspect() {
        return new TelemetryTracingAspect();
    }
}
