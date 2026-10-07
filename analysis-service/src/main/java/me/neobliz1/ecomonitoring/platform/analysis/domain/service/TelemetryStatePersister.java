package me.neobliz1.ecomonitoring.platform.analysis.domain.service;

import static me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.messaging.kafka.processor.util.AggregationUtils.addTelemetryTraceParentId;
import static me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.messaging.kafka.processor.util.AggregationUtils.addTelemetryTransactionIds;
import static me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.messaging.kafka.processor.util.AggregationUtils.getGeohash;
import static me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.messaging.kafka.processor.util.AggregationUtils.getGridCellLayerByteArray;
import static me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.messaging.kafka.processor.util.AggregationUtils.getWeatherMapBuilder;
import static me.neobliz1.ecomonitoring.platform.common.constant.PlatformConstants.TRACE_PROFILE;
import static me.neobliz1.ecomonitoring.platform.common.constant.PlatformConstants.TX_CHAIN_CONFIRMATION_PROFILE;

import lombok.RequiredArgsConstructor;
import lombok.val;
import me.neobliz1.ecomonitoring.platform.analysis.domain.model.AnalysisConstants;
import me.neobliz1.ecomonitoring.platform.analysis.domain.port.outbound.TelemetryPersistenceRepository;
import me.neobliz1.ecomonitoring.platform.analysis.domain.port.outbound.TelemetryPersistentService;
import me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.messaging.kafka.processor.model.ExtractionMatrix;
import me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.messaging.kafka.record.PortWeatherPacket;
import me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.messaging.kafka.record.WeatherMapRecord;
import me.neobliz1.ecomonitoring.platform.analysis.infrastructure.config.AnalysisInfrastructureProperties;
import me.neobliz1.ecomonitoring.platform.model.exception.ProtocolBufferTranslationException;
import org.springframework.beans.factory.annotation.Value;

import java.util.ArrayList;
import java.util.List;

@RequiredArgsConstructor
public class TelemetryStatePersister implements TelemetryPersistentService {

    private final TelemetryPersistenceRepository telemetryRepository;
    private final AnalysisInfrastructureProperties props;

    @Value("#{environment.acceptsProfiles('"+TRACE_PROFILE+"')}")
    private boolean isTraceProfileActive;
    @Value("#{environment.acceptsProfiles('"+TX_CHAIN_CONFIRMATION_PROFILE+"')}")
    private boolean isTxChainCongProfileActive;

    @Override
    public void updateRealTimeSlidingWindow(PortWeatherPacket portWeatherPacket) {
        String geohashKey = getGeohash(portWeatherPacket.latGrid(), portWeatherPacket.lonGrid());
        val weatherPacket = portWeatherPacket.weatherPacket();
        String stationField = AnalysisConstants.HOT_WINDOW_PREFIX+weatherPacket.getStationId();
        String timestampFormatted = String.format(AnalysisConstants.GRID_BUCKET_KEY_FORMAT, weatherPacket.getTimestamp());
        telemetryRepository.saveRealTimeSlidingWindow(geohashKey, stationField, timestampFormatted);
    }

    @Override
    public List<WeatherMapRecord> processAndComputeAggregatedHistory(ExtractionMatrix extractionMatrix) {
        List<WeatherMapRecord> generatedRecords = new ArrayList<>();
        Integer aggregationSecondsPerInterval = props.getKafka().getStreams().getPipeline().getName().getAggregationProcessor().getInterval();
        extractionMatrix.spatialWeatherPacketsByTimestampContainer().forEach((bucketTime, spatialMap) ->
                spatialMap.forEach((spatialKey, packetsList) -> {
                    val weatherMapBuilder = getWeatherMapBuilder(bucketTime, aggregationSecondsPerInterval);
                    if(isTxChainCongProfileActive) {
                        addTelemetryTransactionIds(packetsList, weatherMapBuilder);
                    }
                    if(isTraceProfileActive) {
                        addTelemetryTraceParentId(packetsList, weatherMapBuilder);
                    }
                    byte[] gridCellLayerByteArray = getGridCellLayerByteArray(spatialKey, packetsList, weatherMapBuilder);
                    persistGridCellLayer(spatialKey, gridCellLayerByteArray);
                    generatedRecords.add(new WeatherMapRecord(spatialKey, weatherMapBuilder.build()));
                }));
        return generatedRecords;
    }

    private void persistGridCellLayer(String spatialKey, byte[] gridCellsByteArray) {
        try {
            telemetryRepository.saveHistoricalGridCellLayer(
                    spatialKey,
                    gridCellsByteArray
            );
        } catch(Exception e) {
            throw new ProtocolBufferTranslationException("Domain aggregation encoding sequence failed", e);
        }
    }
}
