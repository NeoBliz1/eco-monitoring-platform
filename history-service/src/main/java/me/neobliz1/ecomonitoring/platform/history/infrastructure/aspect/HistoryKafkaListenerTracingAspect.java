package me.neobliz1.ecomonitoring.platform.history.infrastructure.aspect;

import static me.neobliz1.ecomonitoring.platform.common.constant.PlatformConstants.HISTORICAL_KAFKA_LISTENER_TRACER;
import static me.neobliz1.ecomonitoring.platform.common.constant.PlatformConstants.HISTORICAL_WEATHER_MAP_CONSUMER_MERGE_TELEMETRY;
import static me.neobliz1.ecomonitoring.platform.common.util.PlatformCommonUtils.addLinksToConsumerSpan;
import static me.neobliz1.ecomonitoring.platform.common.util.PlatformCommonUtils.getHeadersTextMapGetter;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import lombok.RequiredArgsConstructor;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.WeatherMap;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;

@Aspect
@RequiredArgsConstructor
public class HistoryKafkaListenerTracingAspect {

    private final Tracer tracer = GlobalOpenTelemetry.getTracer(HISTORICAL_KAFKA_LISTENER_TRACER);

    @Around("execution(public void me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter.inbound.kafka.HistoricalTelemetryListener.consumeHistoricalWeatherMap(..)) && args(record)")
    public Object traceConsumerExecution(ProceedingJoinPoint joinPoint, ConsumerRecord<String, WeatherMap> record) throws Throwable {
        if(record==null || record.value()==null) {
            return joinPoint.proceed();
        }
        WeatherMap weatherMap = record.value();
        Context parentContext = GlobalOpenTelemetry.getPropagators().getTextMapPropagator()
                .extract(Context.current(), weatherMap, getHeadersTextMapGetter());
        SpanBuilder consumerSpanBuilder = tracer.spanBuilder(HISTORICAL_WEATHER_MAP_CONSUMER_MERGE_TELEMETRY)
                .setParent(parentContext)
                .setAttribute("weather.bucket.time", weatherMap.getTimestampBucket());
        addLinksToConsumerSpan(weatherMap, consumerSpanBuilder);
        Span consumerSpan = consumerSpanBuilder.startSpan();
        try(Scope ignored = consumerSpan.makeCurrent()) {
            return joinPoint.proceed();
        } catch(Exception e) {
            consumerSpan.recordException(e);
            consumerSpan.setStatus(StatusCode.ERROR, e.getMessage());
            throw e;
        } finally {
            consumerSpan.end();
        }
    }
}