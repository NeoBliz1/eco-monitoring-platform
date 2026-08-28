package me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.messaging.kafka.processor;

import static me.neobliz1.ecomonitoring.platform.analysis.domain.model.AnalysisConstants.GRID_BUCKET_KEY_FORMAT;
import static me.neobliz1.ecomonitoring.platform.analysis.domain.model.AnalysisConstants.ZERO_LOSS_ACCUMULATION_STORE;
import static me.neobliz1.ecomonitoring.platform.analysis.domain.service.TelemetryUtils.clampLatitude;
import static me.neobliz1.ecomonitoring.platform.analysis.domain.service.TelemetryUtils.clampLongitude;
import static me.neobliz1.ecomonitoring.platform.common.constant.PlatformConstants.HASHTAG_DELIMITER;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import me.neobliz1.ecomonitoring.platform.analysis.domain.port.outbound.TelemetryPersistentService;
import me.neobliz1.ecomonitoring.platform.analysis.domain.service.TelemetryUtils;
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

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@RequiredArgsConstructor
public class TelemetryAggregationProcessor implements Processor<String, WeatherPacket, String, WeatherMap> {

    private final TelemetryPersistentService persistentService;
    private final int secondsPerInterval;
    private KeyValueStore<String, WeatherPacket> accumStore;
    private ProcessorContext<String, WeatherMap> context;
    private long lastStreamTime = Instant.now().toEpochMilli();

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
        String uniqueTxId = packet.getStationId()+":"+packet.getTimestamp();
        // record.key() is already pre-formatted as "55.0#-61.0" by selectKey()
        String storageKey = String.format("%017d", TelemetryUtils.getAggregationBucketFloorInterval(packet.getTimestamp(), secondsPerInterval))
                +HASHTAG_DELIMITER+record.key()+HASHTAG_DELIMITER+uniqueTxId;
        if(log.isDebugEnabled()) {
            log.debug("Storing taskId {}", this.context.taskId().toString());
        }
        accumStore.put(storageKey, packet);
        Location location = packet.getLocation();
        double latGrid = clampLatitude(location.getLatitude());
        double lonGrid = clampLongitude(location.getLongitude());
        persistentService.updateRealTimeSlidingWindow(packet, latGrid, lonGrid);
    }

    private void flushAccumulatedWindows(long currentStreamTimeInMillis) {
        long currentStreamTimeMs = this.context.currentStreamTimeMs();
        if(currentStreamTimeMs<lastStreamTime) {
            return;
        }
        lastStreamTime = currentStreamTimeMs;
        long currentWindowFloor = TelemetryUtils.getAggregationBucketFloorInterval(currentStreamTimeInMillis, secondsPerInterval);
        String startRangeKey = String.format(GRID_BUCKET_KEY_FORMAT, 0);
        String endRangeKey = String.format(GRID_BUCKET_KEY_FORMAT, currentWindowFloor)+"\uFFFF";
        List<String> keysToRemove = new ArrayList<>();
        Map<Long, Map<String, List<WeatherPacket>>> extractionMatrix = new HashMap<>();
        try(KeyValueIterator<String, WeatherPacket> iterator = accumStore.range(startRangeKey, endRangeKey)) {
            while(iterator.hasNext()) {
                KeyValue<String, WeatherPacket> entry = iterator.next();
                String key = entry.key;

                String[] parts = key.split(HASHTAG_DELIMITER);
                long bucketTime = Long.parseLong(parts[0]);

                if(bucketTime>currentWindowFloor) {
                    continue;
                }
                //UTS timestamp + latitude + longitude
                String spatialKey = parts[0]+HASHTAG_DELIMITER+parts[1]+HASHTAG_DELIMITER+parts[2];
                if(log.isDebugEnabled()) {
                    log.debug("Run flushAccumulatedWindows {}", spatialKey);
                }
                extractionMatrix.computeIfAbsent(bucketTime, k -> new HashMap<>())
                        .computeIfAbsent(spatialKey, k -> new ArrayList<>())
                        .add(entry.value);
                keysToRemove.add(key);
            }
        }
        // Forward calculations downstream safely within the active Kafka stream execution runtime task context
        if(!extractionMatrix.isEmpty()) {
            persistentService.processAndComputeAggregatedHistory(extractionMatrix)
                    .forEach(record -> context.forward(new Record<>(record.key(), record.payload(), currentWindowFloor)));
            keysToRemove.forEach(accumStore::delete);
        }
    }
}