package me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.messaging.kafka.processor;

import static me.neobliz1.ecomonitoring.platform.analysis.domain.model.AnalysisConstants.ZERO_LOSS_ACCUMULATION_STORE;
import static me.neobliz1.ecomonitoring.platform.analysis.domain.service.AnalysisUtils.clampLatitude;
import static me.neobliz1.ecomonitoring.platform.analysis.domain.service.AnalysisUtils.clampLongitude;
import static me.neobliz1.ecomonitoring.platform.analysis.domain.service.AnalysisUtils.getAggregationBucketFloorMillisInterval;
import static me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.messaging.kafka.processor.util.AggregationUtils.getAggregationSecondsPerInterval;
import static me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.messaging.kafka.record.ParsedStorageKey.parseAggKey;
import static me.neobliz1.ecomonitoring.platform.common.constant.PlatformConstants.GEOHASH_SEPARATOR;

import jakarta.annotation.PostConstruct;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import me.neobliz1.ecomonitoring.platform.analysis.domain.port.outbound.TelemetryPersistentService;
import me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.messaging.kafka.processor.model.ExtractionMatrix;
import me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.messaging.kafka.record.ParsedStorageKey;
import me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.messaging.kafka.record.PortWeatherPacket;
import me.neobliz1.ecomonitoring.platform.analysis.infrastructure.config.AnalysisInfrastructureProperties;
import me.neobliz1.ecomonitoring.platform.common.util.PlatformContractsUtils;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.Location;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.WeatherPacket;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.WeatherMap;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.jspecify.annotations.NonNull;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@RequiredArgsConstructor
public class WeatherPacketStreamAggregationProcessor implements Processor<String, WeatherPacket, String, WeatherMap> {

    private final AnalysisInfrastructureProperties props;
    private final TelemetryPersistentService persistentService;

    @Getter
    private int secondsPerInterval;
    @Setter
    private WeatherPacketStreamAggregationProcessor self;
    private KeyValueStore<String, WeatherPacket> accumStore;
    private ProcessorContext<String, WeatherMap> context;
    private long lastStreamTime = Instant.now().toEpochMilli();

    @PostConstruct
    public void postConstructInit() {
        this.secondsPerInterval = getAggregationSecondsPerInterval(props);
    }

    @Override
    public void init(ProcessorContext<String, WeatherMap> context) {
        this.context = context;
        this.accumStore = this.context.getStateStore(ZERO_LOSS_ACCUMULATION_STORE);

        this.context.schedule(
                Duration.ofSeconds(secondsPerInterval),
                PunctuationType.STREAM_TIME,
                this::flushAccumulatedWindows
        );
    }

    @Override
    public void process(Record<String, WeatherPacket> record) {
        if(record==null || record.value()==null) {
            return;
        }
        WeatherPacket packet = record.value();
        String uniqueTxId = PlatformContractsUtils.getUniqueTxId(packet);
        String storageKey = String.format("%017d", getAggregationBucketFloorMillisInterval(packet.getTimestamp(), secondsPerInterval))
                +GEOHASH_SEPARATOR+record.key()+GEOHASH_SEPARATOR+uniqueTxId;

        if(log.isDebugEnabled()) {
            log.debug("Storing storageKey {} for task {}", storageKey, context.taskId());
        }
        accumStore.put(storageKey, packet);
        Location location = packet.getLocation();
        double latGrid = clampLatitude(location.getLatitude());
        double lonGrid = clampLongitude(location.getLongitude());
        PortWeatherPacket portWeatherPacket = new PortWeatherPacket(packet, latGrid, lonGrid);
        persistentService.updateRealTimeSlidingWindow(portWeatherPacket);
    }

    private void flushAccumulatedWindows(long currentStreamTimeInMillis) {
        long currentStreamTimeMs = this.context.currentStreamTimeMs();
        if(currentStreamTimeMs<lastStreamTime) {
            return;
        }
        lastStreamTime = currentStreamTimeMs;
        long currentWindowFloor = getAggregationBucketFloorMillisInterval(currentStreamTimeInMillis, secondsPerInterval);
        processDataInAccumStoreByCurrentFloorTimestamp(currentWindowFloor);
    }

    private void processDataInAccumStoreByCurrentFloorTimestamp(long currentWindowFloor) {
        String startRangeKey = String.format("%017d", 0L);
        String endRangeKey = String.format("%017d", currentWindowFloor-1)+GEOHASH_SEPARATOR+"\uFFFF";
        try(KeyValueIterator<String, WeatherPacket> accumStorePacketsByStorageKey = accumStore.range(startRangeKey, endRangeKey)) {
            if(log.isDebugEnabled()) {
                log.debug("Range scan from [{}] to [{}]", startRangeKey, endRangeKey);
            }
            ExtractionMatrix extractionMatrix = createExtractionMatrixFromAccumStorePackets(accumStorePacketsByStorageKey);
            Map<Long, Map<String, List<WeatherPacket>>> spatialWeatherPacketsByTimestampContainer =
                    extractionMatrix.spatialWeatherPacketsByTimestampContainer();
            if(!spatialWeatherPacketsByTimestampContainer.isEmpty()) {
                WeatherPacketStreamAggregationProcessor proxy = (this.self!=null)?this.self:this;
                proxy.executeForwardingAndCleanup(extractionMatrix, currentWindowFloor);
                cleanUpAccumStore(extractionMatrix.keysToRemove());
            }
        } catch(Exception e) {
            log.error("Failed to flush aggregation window {}, : {}", currentWindowFloor, e.getMessage(), e);
        }
    }

    private @NonNull ExtractionMatrix createExtractionMatrixFromAccumStorePackets(
            @NonNull KeyValueIterator<String, WeatherPacket> accumStorePacketsByStorageKey) {
        ExtractionMatrix extractionMatrix = ExtractionMatrix.empty();
        val spatialWeatherPacketsByTimestampContainer = extractionMatrix.spatialWeatherPacketsByTimestampContainer();
        List<String> keysToRemove = extractionMatrix.keysToRemove();
        while(accumStorePacketsByStorageKey.hasNext()) {
            KeyValue<String, WeatherPacket> entry = accumStorePacketsByStorageKey.next();
            String key = entry.key;
            ParsedStorageKey storageKey = parseAggKey(key);
            long bucketTime = Long.parseLong(storageKey.bucketTime());
            String spatialKey = storageKey.spatialKey();
            if(log.isDebugEnabled()) {
                log.debug("Flushing spatialKey {}", spatialKey);
            }
            spatialWeatherPacketsByTimestampContainer.computeIfAbsent(bucketTime, k -> new HashMap<>())
                    .computeIfAbsent(spatialKey, k -> new ArrayList<>())
                    .add(entry.value);
            keysToRemove.add(key);
        }
        return extractionMatrix;
    }

    public void executeForwardingAndCleanup(ExtractionMatrix extractionMatrix, long currentWindowFloor) {
        persistentService.processAndComputeAggregatedHistory(extractionMatrix)
                .forEach(record -> context.forward(new Record<>(record.key(), record.payload(), currentWindowFloor)));
    }

    private void cleanUpAccumStore(List<String> keysToRemove) {
        keysToRemove.forEach(accumStore::delete);
    }
}