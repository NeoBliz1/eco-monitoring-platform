package me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.messaging.kafka.processor.util;

import static me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.messaging.kafka.record.ParsedStorageKey.parseSpatialKey;
import static me.neobliz1.ecomonitoring.platform.common.constant.PlatformConstants.GEOHASH_SEPARATOR;
import static me.neobliz1.ecomonitoring.platform.common.constant.PlatformConstants.TRACE_PARENT_FORMAT;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import lombok.experimental.UtilityClass;
import me.neobliz1.ecomonitoring.platform.analysis.domain.service.TelemetryAnalysisAccumulator;
import me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.messaging.kafka.record.ParsedStorageKey;
import me.neobliz1.ecomonitoring.platform.common.util.PlatformContractsUtils;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.SensorReading;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.WeatherPacket;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.GridCellLayers;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.WeatherMap;
import org.jspecify.annotations.NonNull;

import java.time.Duration;
import java.util.List;

@UtilityClass
public class AggregationUtils {

    public static WeatherMap.@NonNull Builder getWeatherMapBuilder(@NonNull Long bucketTime, @NonNull Integer aggregationSecondsPerInterval) {
        return WeatherMap.newBuilder()
                .setTimestampBucket(bucketTime)
                .setIntervalMinutes((int) Duration.ofSeconds(aggregationSecondsPerInterval).toMinutes());
    }

    public static void addTelemetryTransactionIds(List<WeatherPacket> packetsList, WeatherMap.Builder weatherMapBuilder) {
        for(WeatherPacket packet : packetsList) {
            String uniqueTxId = PlatformContractsUtils.getUniqueTxId(packet);
            weatherMapBuilder.addTelemetryTransactionsId(uniqueTxId);
            addTelemetryTraceParent(packet, weatherMapBuilder);
        }
    }

    public static void addTelemetryTraceParentId(WeatherMap.Builder weatherMapBuilder) {
        SpanContext currentSpanContext = Span.current().getSpanContext();
        if(currentSpanContext.isValid()) {
            String traceParentStr = String.format(TRACE_PARENT_FORMAT,
                    currentSpanContext.getTraceId(),
                    currentSpanContext.getSpanId(),
                    currentSpanContext.getTraceFlags().asHex()
            );
            weatherMapBuilder.setTraceParent(traceParentStr);
        }
    }

    public static void addTelemetryTraceParent(WeatherPacket packet, WeatherMap.Builder weatherMapBuilder) {
        if(packet.hasField(WeatherPacket.getDescriptor().findFieldByNumber(5)) && !packet.getTraceParent().isEmpty()) {
            weatherMapBuilder.addTelemetryTraceParents(packet.getTraceParent());
        }
    }

    public static byte[] getGridCellsByteArray(String spatialKey, List<WeatherPacket> packetsList, WeatherMap.Builder weatherMapBuilder) {
        GridCellLayers.Builder cellBuilder = aggregatePackets(packetsList);
        ParsedStorageKey parsed = parseSpatialKey(spatialKey);
        String geohash = parsed.geohash();
        cellBuilder.setGeohash(geohash);
        weatherMapBuilder.putGridCells(geohash, cellBuilder.build());
        return cellBuilder.build().toByteArray();
    }

    private static GridCellLayers.Builder aggregatePackets(List<WeatherPacket> packets) {
        TelemetryAnalysisAccumulator resultContainer = packets.parallelStream()
                .collect(
                        TelemetryAnalysisAccumulator::new,
                        (container, packet) -> {
                            for(SensorReading reading : packet.getReadingsList()) {
                                container.accumulate(reading);
                            }
                        },
                        TelemetryAnalysisAccumulator::merge
                );

        return resultContainer.applyTo(GridCellLayers.newBuilder().setReadingCount(packets.size()));
    }

    public static @NonNull String getGeohash(double latGrid, double lonGrid) {
        return latGrid+GEOHASH_SEPARATOR+lonGrid;
    }
}
