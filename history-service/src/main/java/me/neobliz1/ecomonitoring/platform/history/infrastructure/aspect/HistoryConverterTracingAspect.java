package me.neobliz1.ecomonitoring.platform.history.infrastructure.aspect;

import static me.neobliz1.ecomonitoring.platform.common.constant.PlatformConstants.HISTORICAL_WEATHER_MAP_CONVERTER_TRACER;
import static me.neobliz1.ecomonitoring.platform.common.constant.PlatformConstants.HISTORICAL_WEATHER_PACKET_MAP_CONVERTER_TRACE_SPAN;
import static me.neobliz1.ecomonitoring.platform.common.util.PlatformCommonUtils.getHeadersTextMapGetter;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import lombok.RequiredArgsConstructor;
import me.neobliz1.ecomonitoring.platform.history.domain.model.entity.WeatherGridCellLayer;
import me.neobliz1.ecomonitoring.platform.history.domain.model.entity.WeatherMapBucket;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.WeatherMap;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;

import java.util.List;

@Aspect
@RequiredArgsConstructor
public class HistoryConverterTracingAspect {

    private final Tracer tracer = GlobalOpenTelemetry.getTracer(HISTORICAL_WEATHER_MAP_CONVERTER_TRACER);

    @Around("execution(public void me.neobliz1.ecomonitoring.platform.history.infrastructure.mapper.WeatherMapConverter.mergeTelemetryInBatch(..)) && args(weatherMap, bucket, targetedCells)")
    public Object traceMergeExecution(
            ProceedingJoinPoint joinPoint,
            WeatherMap weatherMap,
            WeatherMapBucket bucket,
            List<WeatherGridCellLayer> targetedCells) throws Throwable {
        Context parentContext = GlobalOpenTelemetry.getPropagators().getTextMapPropagator()
                .extract(Context.current(), weatherMap, getHeadersTextMapGetter());
        Span mergeSpan = tracer.spanBuilder(HISTORICAL_WEATHER_PACKET_MAP_CONVERTER_TRACE_SPAN)
                .setParent(parentContext)
                .setAttribute("weather.bucket.id", bucket.getId().toString())
                .setAttribute("weather.bucket.timestamp", bucket.getTimestampBucket())
                .setAttribute("weather.grid.cells.count", weatherMap.getGridCellsMap().size())
                .setAttribute("weather.targeted.cells.count", targetedCells.size())
                .startSpan();
        try(Scope ignored = mergeSpan.makeCurrent()) {
            Object result = joinPoint.proceed();
            mergeSpan.setStatus(StatusCode.OK);
            return result;
        } catch(Exception e) {
            mergeSpan.recordException(e);
            mergeSpan.setStatus(StatusCode.ERROR, e.getMessage());
            throw e;
        } finally {
            mergeSpan.end();
        }
    }
}
