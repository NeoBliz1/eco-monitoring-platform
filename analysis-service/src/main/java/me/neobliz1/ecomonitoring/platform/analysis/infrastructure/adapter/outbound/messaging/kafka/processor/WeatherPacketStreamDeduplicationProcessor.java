package me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.messaging.kafka.processor;

import static me.neobliz1.ecomonitoring.platform.analysis.domain.model.AnalysisConstants.DEDUPLICATE_ROCKS_DB;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import me.neobliz1.ecomonitoring.platform.common.util.PlatformContractsUtils;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.WeatherPacket;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

@Slf4j
@RequiredArgsConstructor
public class WeatherPacketStreamDeduplicationProcessor implements Processor<String, WeatherPacket, String, WeatherPacket> {

    @Getter
    private final long deduplicationInterval;
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
        String uniqueTxId = PlatformContractsUtils.getUniqueTxId(packet);
        long recordTimestamp = record.timestamp();
        try(WindowStoreIterator<String> iterator = deduplicateStore.fetch(
                uniqueTxId,
                recordTimestamp-deduplicationInterval,
                recordTimestamp+deduplicationInterval)) {
            if(iterator.hasNext()) {
                log.warn("Duplicate record found for txId: {}", uniqueTxId);
                return;
            }
        }
        deduplicateStore.put(uniqueTxId, "COMMITTED", recordTimestamp);
        context.forward(record);
    }
}