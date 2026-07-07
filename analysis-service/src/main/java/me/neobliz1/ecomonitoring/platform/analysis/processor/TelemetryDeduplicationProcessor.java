package me.neobliz1.ecomonitoring.platform.analysis.processor;

import lombok.RequiredArgsConstructor;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.WeatherPacket;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

@RequiredArgsConstructor
public class TelemetryDeduplicationProcessor implements Processor<String, WeatherPacket, String, WeatherPacket> {

    public static final String DEDUPLICATE_ROCKS_DB = "embedded-deduplicate-rocks-db";
    private static final long DEDUPLICATION_INTERVAL_MS = 600_000L; // 10 min

    private WindowStore<String, String> deduplicateStore;
    private ProcessorContext<String, WeatherPacket> context;

    @Override
    public void init(ProcessorContext<String, WeatherPacket> context) {
        this.context = context;
        this.deduplicateStore = context.getStateStore(DEDUPLICATE_ROCKS_DB);
    }

    @Override
    public void process(Record<String, WeatherPacket> record) {
        if(record==null || record.value()==null) {
            return;
        }

        WeatherPacket packet = record.value();
        String uniqueTxId = packet.getStationId()+":"+packet.getTimestamp();
        long recordTimestamp = record.timestamp();

        // 1. Check local sliding window for duplicates
        try(WindowStoreIterator<String> iterator = deduplicateStore.fetch(
                uniqueTxId,
                recordTimestamp-DEDUPLICATION_INTERVAL_MS,
                recordTimestamp+DEDUPLICATION_INTERVAL_MS)) {
            if(iterator.hasNext()) {
                return; // Silently drop record to satisfy exactly-once semantics
            }
        }

        // 2. Mark as processed and forward down to intermediate topic (environment.weather.raw)
        deduplicateStore.put(uniqueTxId, "COMMITTED", recordTimestamp);
        context.forward(record);
    }
}

