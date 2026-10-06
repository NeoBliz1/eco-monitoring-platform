package me.neobliz1.ecomonitoring.platform.analysis.infrastructure.advice;

import static me.neobliz1.ecomonitoring.platform.common.constant.PlatformConstants.ANALYSIS_DEDUPLICATE_SPAN;
import static me.neobliz1.ecomonitoring.platform.common.util.PlatformCommonUtils.getWeatherPacketTextMapGetter;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.messaging.kafka.processor.WeatherPacketStreamDeduplicationProcessor;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.WeatherPacket;
import org.apache.kafka.streams.processor.api.Record;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;

@Slf4j
@Aspect
@RequiredArgsConstructor
public class TelemetryDeduplicationTracingAspect {

    private final Tracer tracer;

    @Around("execution(public void me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.messaging.kafka.processor.TelemetryDeduplicationProcessor.process(..)) && args(record)")
    public Object traceDeduplicationExecution(ProceedingJoinPoint joinPoint, Record<String, WeatherPacket> record) throws Throwable {
        if(record==null || record.value()==null) {
            return joinPoint.proceed();
        }
        WeatherPacket packet = record.value();
        WeatherPacketStreamDeduplicationProcessor processor = (WeatherPacketStreamDeduplicationProcessor) joinPoint.getTarget();
        Context extractedContext = GlobalOpenTelemetry.getPropagators().getTextMapPropagator()
                .extract(Context.current(), packet, getWeatherPacketTextMapGetter());
        Span streamSpan = tracer.spanBuilder(ANALYSIS_DEDUPLICATE_SPAN)
                .setParent(extractedContext)
                .setAttribute("station.id", packet.getStationId())
                .setAttribute("deduplication.interval.ms", processor.getDeduplicationInterval())
                .startSpan();
        try(Scope ignored = streamSpan.makeCurrent()) {
            return joinPoint.proceed();
        } catch(Exception e) {
            streamSpan.recordException(e);
            streamSpan.setStatus(StatusCode.ERROR, e.getMessage());
            throw e;
        } finally {
            streamSpan.end();
        }
    }
}