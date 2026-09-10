package me.neobliz1.ecomonitoring.platform.analysis.domain.service;

import static me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.messaging.kafka.processor.util.AggregationUtils.addTelemetryTransactionIds;
import static me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.messaging.kafka.processor.util.AggregationUtils.getGridCellsByteArray;
import static me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.messaging.kafka.processor.util.AggregationUtils.getWeatherMapBuilder;
import static me.neobliz1.ecomonitoring.platform.common.constant.PlatformConstants.HASHTAG_DELIMITER;

import lombok.RequiredArgsConstructor;
import lombok.val;
import me.neobliz1.ecomonitoring.platform.analysis.domain.model.AnalysisConstants;
import me.neobliz1.ecomonitoring.platform.analysis.domain.port.outbound.TelemetryPersistenceRepository;
import me.neobliz1.ecomonitoring.platform.analysis.domain.port.outbound.TelemetryPersistentService;
import me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.messaging.kafka.record.WeatherMapRecord;
import me.neobliz1.ecomonitoring.platform.model.exception.ProtocolBufferTranslationException;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.WeatherPacket;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@RequiredArgsConstructor
public class TelemetryStatePersister implements TelemetryPersistentService {

    private final TelemetryPersistenceRepository telemetryRepository;
    private final Integer aggregationSecondsPerInterval;

    @Override
    public void updateRealTimeSlidingWindow(WeatherPacket packet, double latGrid, double lonGrid) {
        String geohashKey = latGrid+HASHTAG_DELIMITER+lonGrid;
        String stationField = AnalysisConstants.HOT_WINDOW_PREFIX+packet.getStationId();
        String timestampFormatted = String.format(AnalysisConstants.GRID_BUCKET_KEY_FORMAT, packet.getTimestamp());
        telemetryRepository.saveRealTimeSlidingWindow(geohashKey, stationField, timestampFormatted);
    }

    @Override
    public List<WeatherMapRecord> processAndComputeAggregatedHistory(Map<Long, Map<String, List<WeatherPacket>>> extractionMatrix) {
        List<WeatherMapRecord> generatedRecords = new ArrayList<>();

        extractionMatrix.forEach((bucketTime, spatialMap) ->
                spatialMap.forEach((spatialKey, packetsList) -> {

                    val weatherMapBuilder = getWeatherMapBuilder(bucketTime, aggregationSecondsPerInterval);
                    addTelemetryTransactionIds(packetsList, weatherMapBuilder);
                    byte[] gridCellsByteArray = getGridCellsByteArray(spatialKey, packetsList, weatherMapBuilder);

                    try {
                        telemetryRepository.saveHistoricalGridCell(
                                spatialKey,
                                gridCellsByteArray
                        );
                    } catch(Exception e) {
                        throw new ProtocolBufferTranslationException("Domain aggregation encoding sequence failed", e);
                    }

                    generatedRecords.add(new WeatherMapRecord(spatialKey, weatherMapBuilder.build()));
                }));

        return generatedRecords;
    }
}
