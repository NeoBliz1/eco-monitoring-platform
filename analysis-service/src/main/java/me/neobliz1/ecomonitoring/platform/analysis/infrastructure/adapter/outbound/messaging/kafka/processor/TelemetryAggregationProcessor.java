package me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.messaging.kafka.processor;

import static me.neobliz1.ecomonitoring.platform.analysis.domain.model.AnalysisConstants.ZERO_LOSS_ACCUMULATION_STORE;
import static me.neobliz1.ecomonitoring.platform.analysis.domain.service.TelemetryUtils.clampLatitude;
import static me.neobliz1.ecomonitoring.platform.analysis.domain.service.TelemetryUtils.clampLongitude;
import static me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.messaging.kafka.processor.util.AggregationUtils.validateSpatialKey;
import static me.neobliz1.ecomonitoring.platform.common.constant.PlatformConstants.HASHTAG_DELIMITER;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;
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
import org.jetbrains.annotations.NotNull;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
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
    private static final Tracer tracer = GlobalOpenTelemetry.getTracer("weather-analysis-topology");

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

    private static final TextMapGetter<WeatherPacket> protoGetter = new TextMapGetter<>() {
        @Override
        public Iterable<String> keys(@NotNull WeatherPacket carrier) {
            return Collections.singletonList("traceparent");
        }

        @Override
        public String get(WeatherPacket carrier, @NotNull String key) {
            if("traceparent".equals(key) && carrier.hasField(WeatherPacket.getDescriptor().findFieldByNumber(5))) {
                return carrier.getTraceParent();
            }
            return null;
        }
    };

    @Override
    public void process(Record<String, WeatherPacket> record) {
        if(record==null || record.value()==null) {
            return;
        }
        WeatherPacket packet = record.value();
        Span streamSpan = getStreamSpan(record, packet);
        try(Scope ignored = streamSpan.makeCurrent()) {
            String uniqueTxId = packet.getStationId()+":"+packet.getTimestamp();
            String storageKey = String.format("%017d", TelemetryUtils.getAggregationBucketFloorInterval(packet.getTimestamp(), secondsPerInterval))
                    +HASHTAG_DELIMITER+record.key()+HASHTAG_DELIMITER+uniqueTxId;
            if(log.isDebugEnabled()) {
                log.debug("Storing taskId {}, Storing storageKey {}", this.context.taskId().toString(), storageKey);
            }
            accumStore.put(storageKey, packet);
            Location location = packet.getLocation();
            double latGrid = clampLatitude(location.getLatitude());
            double lonGrid = clampLongitude(location.getLongitude());
            persistentService.updateRealTimeSlidingWindow(packet, latGrid, lonGrid);
        } catch(Exception e) {
            streamSpan.recordException(e);
            streamSpan.setStatus(StatusCode.ERROR, e.getMessage());
            throw e;
        } finally {
            streamSpan.end();
        }
    }

    private void flushAccumulatedWindows(long currentStreamTimeInMillis) {
        long currentStreamTimeMs = this.context.currentStreamTimeMs();
        if(currentStreamTimeMs<lastStreamTime) {
            return;
        }
        lastStreamTime = currentStreamTimeMs;
        long currentWindowFloor = TelemetryUtils.getAggregationBucketFloorInterval(currentStreamTimeInMillis, secondsPerInterval);
        List<String> keysToRemove = new ArrayList<>();
        Map<Long, Map<String, List<WeatherPacket>>> extractionMatrix = new HashMap<>();
        String startRangeKey = String.format("%017d", 0L);
        String endRangeKey = String.format("%017d", currentWindowFloor-1)+HASHTAG_DELIMITER+"\uFFFF";
        if(log.isDebugEnabled()) {
            log.debug("Executing targeted state store range scan from key [{}] to [{}]", startRangeKey, endRangeKey);
        }
        try(KeyValueIterator<String, WeatherPacket> iterator = accumStore.range(startRangeKey, endRangeKey)) {
            while(iterator.hasNext()) {
                KeyValue<String, WeatherPacket> entry = iterator.next();
                String key = entry.key;

                String[] parts = key.split(HASHTAG_DELIMITER);
                validateSpatialKey(parts, key);
                long bucketTime = Long.parseLong(parts[0]);

                String spatialKey = parts[0]+HASHTAG_DELIMITER+parts[1]+HASHTAG_DELIMITER+parts[2];
                if(log.isDebugEnabled()) {
                    log.debug("Run flushAccumulatedWindows {}", spatialKey);
                }
                extractionMatrix.computeIfAbsent(bucketTime, k -> new HashMap<>())
                        .computeIfAbsent(spatialKey, k -> new ArrayList<>())
                        .add(entry.value);
                keysToRemove.add(key);
            }
        } catch(Exception e) {
            log.error("Processing of the current package {} has fallen, error msg: {}", currentWindowFloor, e.getMessage());
        }

        if(!extractionMatrix.isEmpty()) {
            persistentService.processAndComputeAggregatedHistory(extractionMatrix)
                    .forEach(record -> context.forward(new Record<>(record.key(), record.payload(), currentWindowFloor)));
            keysToRemove.forEach(accumStore::delete);
        }
    }

    private Span getStreamSpan(Record<String, WeatherPacket> record, WeatherPacket packet) {
        Context extractedContext = GlobalOpenTelemetry.getPropagators().getTextMapPropagator()
                .extract(Context.current(), packet, protoGetter);
        return tracer.spanBuilder("KafkaStreams_Aggregate_Tick")
                .setParent(extractedContext)
                .setAttribute("station.id", packet.getStationId())
                .setAttribute("kafka.record.key", record.key())
                .setAttribute("aggregation.interval.secs", secondsPerInterval)
                .startSpan();
    }
}