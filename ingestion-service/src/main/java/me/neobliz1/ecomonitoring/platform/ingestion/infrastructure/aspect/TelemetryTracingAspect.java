package me.neobliz1.ecomonitoring.platform.ingestion.infrastructure.aspect;

import static me.neobliz1.ecomonitoring.platform.common.constant.PlatformConstants.TRACE_PARENT_FORMAT;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.WeatherPacket;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.jetbrains.annotations.NotNull;
import org.jspecify.annotations.NonNull;

@Aspect
public class TelemetryTracingAspect {

    private static @NotNull WeatherPacket injectTraceStringToWeatherPacket(WeatherPacket packet) {
        SpanContext activeContext = Span.current().getSpanContext();
        String traceParentString = String.format(TRACE_PARENT_FORMAT,
                activeContext.getTraceId(),
                activeContext.getSpanId(),
                activeContext.getTraceFlags().asHex());
        return WeatherPacket.newBuilder(packet)
                .setTraceParent(traceParentString)
                .build();
    }

    @Around("execution(* me.neobliz1.ecomonitoring.platform.ingestion.infrastructure.adapter.outbound.grpc.vector.TelemetryPayloadMapper.toPushRequest(..)) && args(packet)")
    public Object injectTraceBeforeMapping(ProceedingJoinPoint joinPoint, @NonNull WeatherPacket packet) throws Throwable {
        WeatherPacket tracedPacket = injectTraceStringToWeatherPacket(packet);
        return joinPoint.proceed(new Object[]{ tracedPacket });
    }
}
