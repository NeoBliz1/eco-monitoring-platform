package me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.messaging.kafka.processor.util;

import lombok.experimental.UtilityClass;
import me.neobliz1.ecomonitoring.platform.analysis.domain.service.TelemetryAnalysisAccumulator;
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
        }
    }

    public static byte[] getGridCellsByteArray(String spatialKey, List<WeatherPacket> packetsList, WeatherMap.Builder weatherMapBuilder) {
        GridCellLayers.Builder cellBuilder = aggregatePackets(packetsList);
        cellBuilder.setGeohash(spatialKey);
        weatherMapBuilder.putGridCells(spatialKey, cellBuilder.build());
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

    public static void validateSpatialKey(String[] parts, String key) {
        if(parts.length<3) throw new IndexOutOfBoundsException("Key format has been invalid: "+key);
    }
}
