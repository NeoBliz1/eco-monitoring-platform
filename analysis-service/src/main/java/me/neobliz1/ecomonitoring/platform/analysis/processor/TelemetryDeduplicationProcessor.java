package me.neobliz1.ecomonitoring.platform.analysis.processor;

import static me.neobliz1.ecomonitoring.platform.analysis.constants.AnalysisConstants.DEDUPLICATE_ROCKS_DB;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.WeatherPacket;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

@Slf4j
@RequiredArgsConstructor
public class TelemetryDeduplicationProcessor implements Processor<String, WeatherPacket, String, WeatherPacket> {

    private WindowStore<String, String> deduplicateStore;
    private ProcessorContext<String, WeatherPacket> context;
    private final long deduplication_interval;

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
                recordTimestamp-deduplication_interval,
                recordTimestamp+deduplication_interval)) {
            if(iterator.hasNext()) {
                log.warn("Duplicate record found for txId: {}", uniqueTxId);
                return;
            }
        }

        // 2. Mark as processed and forward down
        deduplicateStore.put(uniqueTxId, "COMMITTED", recordTimestamp);
        context.forward(record);
    }
}

