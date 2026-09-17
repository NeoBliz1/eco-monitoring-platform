package me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.messaging.kafka.processor;

import static me.neobliz1.ecomonitoring.platform.analysis.domain.model.AnalysisConstants.DEDUPLICATE_ROCKS_DB;
import static me.neobliz1.ecomonitoring.platform.common.util.PlatformCommonUtils.getWeatherPacketTextMapGetter;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
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
public class TelemetryDeduplicationProcessor implements Processor<String, WeatherPacket, String, WeatherPacket> {

    private WindowStore<String, String> deduplicateStore;
    private ProcessorContext<String, WeatherPacket> context;
    private final long deduplication_interval;
    private final Tracer tracer;

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
        Span streamSpan = getStreamSpan(record, packet);
        try(Scope ignored = streamSpan.makeCurrent()) {

            long recordTimestamp = record.timestamp();
            try(WindowStoreIterator<String> iterator = deduplicateStore.fetch(
                    uniqueTxId,
                    recordTimestamp-deduplication_interval,
                    recordTimestamp+deduplication_interval)) {
                if(iterator.hasNext()) {
                    log.warn("Duplicate record found for txId: {}", uniqueTxId);
                    return;
                }
            }
            deduplicateStore.put(uniqueTxId, "COMMITTED", recordTimestamp);
            context.forward(record);
        } catch(Exception e) {
            streamSpan.recordException(e);
            streamSpan.setStatus(StatusCode.ERROR, e.getMessage());
            throw e;
        } finally {
            streamSpan.end();
        }
    }

    private Span getStreamSpan(Record<String, WeatherPacket> record, WeatherPacket packet) {
        Context extractedContext = GlobalOpenTelemetry.getPropagators().getTextMapPropagator()
                .extract(Context.current(), packet, getWeatherPacketTextMapGetter());
        return tracer.spanBuilder("KafkaStreams_Deduplicate_Record")
                .setParent(extractedContext)
                .setAttribute("station.id", packet.getStationId())
                .setAttribute("kafka.record.key", record.key())
                .setAttribute("deduplication.interval.ms", deduplication_interval)
                .startSpan();
    }
}

