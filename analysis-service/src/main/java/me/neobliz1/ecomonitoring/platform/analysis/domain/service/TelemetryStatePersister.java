package me.neobliz1.ecomonitoring.platform.analysis.domain.service;

import static me.neobliz1.ecomonitoring.platform.common.constant.PlatformConstants.HASHTAG_DELIMITER;

import lombok.RequiredArgsConstructor;
import me.neobliz1.ecomonitoring.platform.analysis.domain.model.AnalysisConstants;
import me.neobliz1.ecomonitoring.platform.analysis.domain.port.outbound.TelemetryPersistenceRepository;
import me.neobliz1.ecomonitoring.platform.analysis.domain.port.outbound.TelemetryPersistentService;
import me.neobliz1.ecomonitoring.platform.model.exception.ProtocolBufferTranslationException;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.SensorReading;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.WeatherPacket;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.GridCellLayers;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.WeatherMap;
import org.springframework.beans.factory.annotation.Value;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@RequiredArgsConstructor
public class TelemetryStatePersister implements TelemetryPersistentService {

    private final TelemetryPersistenceRepository telemetryRepository;
    @Value("${spring.kafka.streams.pipeline.name.aggregation-processor.interval}")
    private Integer aggregationSecondsPerInterval;

    @Override
    public void updateRealTimeSlidingWindow(WeatherPacket packet, double latGrid, double lonGrid) {
        String geohashKey = latGrid+HASHTAG_DELIMITER+lonGrid;
        String stationField = AnalysisConstants.HOT_WINDOW_PREFIX+packet.getStationId();
        String timestampFormatted = String.format(AnalysisConstants.UTC_TIMESTAMP_FORMAT, packet.getTimestamp());

        // Delegate technical write execution across the boundary port
        telemetryRepository.saveRealTimeSlidingWindow(geohashKey, stationField, timestampFormatted);
    }

    @Override
    public List<WeatherMapRecord> processAndComputeAggregatedHistory(Map<Long, Map<String, List<WeatherPacket>>> extractionMatrix) {
        List<WeatherMapRecord> generatedRecords = new ArrayList<>();

        extractionMatrix.forEach((bucketTime, spatialMap) -> {
            WeatherMap.Builder weatherMapBuilder = WeatherMap.newBuilder()
                    .setTimestampBucket(bucketTime)
                    .setIntervalMinutes((int) Duration.ofSeconds(aggregationSecondsPerInterval).toMinutes());

            spatialMap.forEach((spatialKey, packetsList) -> {
                GridCellLayers.Builder cellBuilder = aggregatePackets(packetsList);
                cellBuilder.setGeohash(spatialKey);
                weatherMapBuilder.putGridCells(spatialKey, cellBuilder.build());
            });

            WeatherMap finalReport = weatherMapBuilder.build();
            long floorBucketInterval = TelemetryUtils.getAggregationBucketFloorInterval(bucketTime, aggregationSecondsPerInterval);

            // 1. Persist computed data layers via the Port interface boundary
            finalReport.getGridCellsMap().forEach((geohash, cellLayers) -> {
                try {
                    telemetryRepository.saveHistoricalGridCell(
                            String.valueOf(floorBucketInterval),
                            geohash,
                            cellLayers.toByteArray()
                    );
                } catch(Exception e) {
                    throw new ProtocolBufferTranslationException("Domain aggregation encoding sequence failed", e);
                }
            });

            // 2. Stage the output data so infrastructure can forward it to the stream engine
            generatedRecords.add(new WeatherMapRecord(String.valueOf(bucketTime), finalReport));
        });

        return generatedRecords;
    }

    private GridCellLayers.Builder aggregatePackets(List<WeatherPacket> packets) {
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

    public record WeatherMapRecord(String key, WeatherMap payload) {
    }
}
