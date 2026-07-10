package me.neobliz1.ecomonitoring.platform.analysis.processor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import me.neobliz1.ecomonitoring.platform.analysis.service.TelemetryAnalysisService;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@RequiredArgsConstructor
public class TelemetryAggregationProcessor implements Processor<String, WeatherPacket, String, WeatherMap> {

    public static final String ZERO_LOSS_ACCUMULATION_STORE = "zero-loss-accumulation-store";
    private static final long BUFFERING_INTERVAL_MS = 300_000L; // 5 min

    private final TelemetryAnalysisService service;
    private final int interval;
    private KeyValueStore<String, WeatherPacket> accumStore;
    private ProcessorContext<String, WeatherMap> context;

    @Override
    public void init(ProcessorContext<String, WeatherMap> context) {
        this.context = context;
        this.accumStore = this.context.getStateStore(ZERO_LOSS_ACCUMULATION_STORE);

        this.context.schedule(
                Duration.ofSeconds(interval),
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
        long packetTimestamp = packet.getTimestamp();
        long utcFiveMinuteBucketFloor = packetTimestamp-(packetTimestamp%BUFFERING_INTERVAL_MS);

        double latGrid = Math.round(packet.getLocation().getLatitude()*10.0)/10.0;
        double lonGrid = Math.round(packet.getLocation().getLongitude()*10.0)/10.0;
        String uniqueTxId = packet.getStationId()+":"+packetTimestamp;

        // 1. Dynamic storage key mapping ensuring optimal range-scans
        String storageKey = String.format("%017d_%.1f_%.1f_%s", utcFiveMinuteBucketFloor, latGrid, lonGrid, uniqueTxId);
        accumStore.put(storageKey, packet);

        // 2. Real-time idempotent cache write-through target
        this.service.updateRealTimeSlidingWindow(packet, latGrid, lonGrid);
    }

    private void flushAccumulatedWindows(long currentStreamTime) {
        long currentWallClockFloor = (currentStreamTime/BUFFERING_INTERVAL_MS)*BUFFERING_INTERVAL_MS;

        String startRangeKey = String.format("%017d", 0);
        String endRangeKey = String.format("%017d", currentWallClockFloor);

        List<String> keysToRemove = new ArrayList<>();
        Map<Long, Map<String, List<WeatherPacket>>> extractionMatrix = new HashMap<>();

        try(KeyValueIterator<String, WeatherPacket> iterator = accumStore.range(startRangeKey, endRangeKey)) {
            while(iterator.hasNext()) {
                KeyValue<String, WeatherPacket> entry = iterator.next();
                String underscore = "_";
                String[] parts = entry.key.split(underscore);
                long bucketTime = Long.parseLong(parts[0]);

                if(bucketTime>=currentWallClockFloor) {
                    continue;
                }
                String spatialKey = parts[0]
                        +underscore
                        +parts[1]
                        +underscore
                        +parts[2];
                log.info("Run flushAccumulatedWindows {}", spatialKey);

                extractionMatrix.computeIfAbsent(bucketTime, k -> new HashMap<>())
                        .computeIfAbsent(spatialKey, k -> new ArrayList<>())
                        .add(entry.value);

                keysToRemove.add(entry.key);
            }
        }

        // Purge collected keys atomically inside the current transaction
        keysToRemove.forEach(accumStore::delete);

        // Hand off structured calculations safely to the service layer for history topic emission
        if(!extractionMatrix.isEmpty()) {
            this.service.persistAggregatedHistory(extractionMatrix, this.context, currentWallClockFloor);
        }
    }
}

