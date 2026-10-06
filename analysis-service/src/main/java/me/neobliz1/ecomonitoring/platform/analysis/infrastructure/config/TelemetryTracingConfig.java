package me.neobliz1.ecomonitoring.platform.analysis.infrastructure.config;

import static me.neobliz1.ecomonitoring.platform.common.constant.PlatformConstants.ANALYSIS_WEATHER_TOPOLOGY_TRACER;
import static me.neobliz1.ecomonitoring.platform.common.constant.PlatformConstants.TRACE_PROFILE;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import me.neobliz1.ecomonitoring.platform.analysis.infrastructure.advice.TelemetryAggregationTracingAspect;
import me.neobliz1.ecomonitoring.platform.analysis.infrastructure.advice.TelemetryDeduplicationTracingAspect;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.context.annotation.Profile;
import org.springframework.context.annotation.Scope;

@Configuration
@Profile(TRACE_PROFILE)
@EnableAspectJAutoProxy
public class TelemetryTracingConfig {

    private final Tracer tracer = GlobalOpenTelemetry.getTracer(ANALYSIS_WEATHER_TOPOLOGY_TRACER);

    @Bean
    @Scope("prototype")
    public TelemetryDeduplicationTracingAspect telemetryDeduplicationTracingAspect() {
        return new TelemetryDeduplicationTracingAspect(tracer);
    }

    @Bean
    @Scope("prototype")
    public TelemetryAggregationTracingAspect telemetryAggregationTracingAspect() {
        return new TelemetryAggregationTracingAspect(tracer);
    }
}