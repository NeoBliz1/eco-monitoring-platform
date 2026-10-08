package me.neobliz1.ecomonitoring.platform.analysis.infrastructure.advice;

import static me.neobliz1.ecomonitoring.platform.common.constant.PlatformConstants.ANALYSIS_AGGREGATE_SPAN;
import static me.neobliz1.ecomonitoring.platform.common.constant.PlatformConstants.ANALYSIS_FLUSH_AGGREGATION_WINDOW_SPAN;
import static me.neobliz1.ecomonitoring.platform.common.util.PlatformCommonUtils.getWeatherPacketTextMapGetter;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.messaging.kafka.processor.WeatherPacketStreamAggregationProcessor;
import me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.messaging.kafka.processor.model.ExtractionMatrix;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.WeatherPacket;
import org.apache.kafka.streams.processor.api.Record;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.jspecify.annotations.NonNull;

import java.util.List;
import java.util.stream.Stream;

@Slf4j
@Aspect
@RequiredArgsConstructor
public class TelemetryAggregationTracingAspect {

    private final Tracer tracer;

    @Around("execution(public void me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.messaging.kafka.processor.WeatherPacketStreamAggregationProcessor.process(..)) && args(record)")
    public Object traceProcessExecution(ProceedingJoinPoint joinPoint, Record<String, WeatherPacket> record) throws Throwable {
        if(record==null || record.value()==null) {
            return joinPoint.proceed();
        }
        WeatherPacket packet = record.value();
        WeatherPacketStreamAggregationProcessor processor = (WeatherPacketStreamAggregationProcessor) joinPoint.getTarget();
        Context extractedContext = GlobalOpenTelemetry.getPropagators().getTextMapPropagator()
                .extract(Context.current(), packet, getWeatherPacketTextMapGetter());
        Span streamSpan = tracer.spanBuilder(ANALYSIS_AGGREGATE_SPAN)
                .setParent(extractedContext)
                .setAttribute("station.id", packet.getStationId())
                .setAttribute("aggregation.interval.secs", processor.getSecondsPerInterval())
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

    @Around("execution(public void me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.messaging.kafka.processor.WeatherPacketStreamAggregationProcessor.executeForwardingAndCleanup(..)) && args(extractionMatrix, currentWindowFloor)")
    public Object traceFlushExecution(
            ProceedingJoinPoint joinPoint,
            ExtractionMatrix extractionMatrix,
            long currentWindowFloor) throws Throwable {
        WeatherPacket anchorPacket = getWeatherPacketStream(extractionMatrix)
                .findFirst()
                .orElse(null);
        var flushSpanBuilder = tracer.spanBuilder(ANALYSIS_FLUSH_AGGREGATION_WINDOW_SPAN)
                .setAttribute("window.floor.ms", currentWindowFloor);
        if(anchorPacket!=null && !anchorPacket.getTraceParent().isEmpty()) {
            Context anchorContext = GlobalOpenTelemetry.getPropagators().getTextMapPropagator()
                    .extract(Context.current(), anchorPacket, getWeatherPacketTextMapGetter());
            flushSpanBuilder.setParent(anchorContext);
        } else {
            return joinPoint.proceed();
        }
        addLinkToTraceParentForEachMatrixPacket(extractionMatrix, flushSpanBuilder);
        Span flushSpan = flushSpanBuilder.startSpan();
        try(Scope ignored = flushSpan.makeCurrent()) {
            return joinPoint.proceed();
        } catch(Exception ex) {
            flushSpan.recordException(ex);
            flushSpan.setStatus(StatusCode.ERROR, ex.getMessage());
            throw ex;
        } finally {
            flushSpan.end();
        }
    }

    private void addLinkToTraceParentForEachMatrixPacket(ExtractionMatrix extractionMatrix, SpanBuilder flushSpanBuilder) {
        getWeatherPacketStream(extractionMatrix)
                .skip(1)
                .forEach(packet -> {
                    String tp = packet.getTraceParent();
                    if(!tp.isEmpty()) {
                        try {
                            String[] parts = tp.split("-");
                            if(parts.length>=4) {
                                SpanContext pktSpanContext = SpanContext.create(parts[1], parts[2], TraceFlags.getSampled(),
                                        TraceState.getDefault());
                                if(pktSpanContext.isValid()) {
                                    flushSpanBuilder.addLink(pktSpanContext);
                                }
                            }
                        } catch(Exception ignored) {
                        }
                    }
                });
    }

    public @NonNull Stream<WeatherPacket> getWeatherPacketStream(ExtractionMatrix extractionMatrix) {
        return extractionMatrix.spatialWeatherPacketsByTimestampContainer().values().stream()
                .flatMap(m -> m.values().stream())
                .flatMap(List::stream);
    }
}