package me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.messaging.kafka;

import static me.neobliz1.ecomonitoring.platform.analysis.domain.service.TelemetryUtils.clampLatitude;
import static me.neobliz1.ecomonitoring.platform.analysis.domain.service.TelemetryUtils.clampLongitude;
import static me.neobliz1.ecomonitoring.platform.common.constant.PlatformConstants.HASHTAG_DELIMITER;
import static me.neobliz1.ecomonitoring.platform.common.constant.PlatformConstants.SCHEMA_REGISTRY_URL;

import io.confluent.kafka.streams.serdes.protobuf.KafkaProtobufSerde;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import me.neobliz1.ecomonitoring.platform.analysis.domain.model.AnalysisConstants;
import me.neobliz1.ecomonitoring.platform.analysis.domain.port.inbound.TelemetryAnalysisService;
import me.neobliz1.ecomonitoring.platform.analysis.domain.port.outbound.TelemetryPersistentService;
import me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.messaging.kafka.processor.TelemetryAggregationProcessor;
import me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.messaging.kafka.processor.TelemetryDeduplicationProcessor;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.Location;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.WeatherPacket;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.WeatherMap;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.jspecify.annotations.NonNull;
import org.springframework.beans.factory.annotation.Value;

import java.time.Duration;
import java.util.Map;

@Slf4j
@RequiredArgsConstructor
public class TelemetryTopologyOrchestrator implements TelemetryAnalysisService {

    private final TelemetryPersistentService persistentService;

    @Value("${spring.kafka.topic.weather-live}")
    private String kafkaIngestionLiveTopic;
    @Value("${spring.kafka.topic.weather-raw}")
    private String kafkaAnalysisRawTopic;
    @Value("${spring.kafka.topic.weather-history}")
    private String kafkaAnalysisHistoryTopic;
    @Getter
    @Value("${spring.kafka.streams.pipeline.name.aggregation-processor.interval}")
    private Integer aggregationSecondsPerInterval;
    @Value("${spring.kafka.streams.pipeline.name.deduplication-processor.interval}")
    private Long deduplicationInterval;
    @Value("${spring.kafka.streams.properties.schema.registry.url}")
    private String schemaRegistryUrl;
    private Serde<WeatherPacket> weatherPacketSerde;

    @Override
    public KStream<String, WeatherPacket> buildTopology(StreamsBuilder streamsBuilder) {
        Map<String, String> serdeConfig = Map.of(SCHEMA_REGISTRY_URL, schemaRegistryUrl);
        weatherPacketSerde = new KafkaProtobufSerde<>(WeatherPacket.class);
        weatherPacketSerde.configure(serdeConfig, false);
        registerTransactionalStateStores(streamsBuilder);
        KStream<String, WeatherPacket> deduplicatedStream = runTransactionalDeduplicationPipeline(streamsBuilder);
        runTransactionalAggregationStream(deduplicatedStream, serdeConfig);
        return deduplicatedStream;
    }

    private void registerTransactionalStateStores(StreamsBuilder streamsBuilder) {
        registerDeduplicationStore(streamsBuilder);
        registerAggregationStore(streamsBuilder);
    }

    private void registerDeduplicationStore(StreamsBuilder streamsBuilder) {
        StoreBuilder<WindowStore<String, String>> dedupStoreBuilder = Stores.windowStoreBuilder(
                Stores.persistentWindowStore(
                        AnalysisConstants.DEDUPLICATE_ROCKS_DB,
                        Duration.ofMillis(deduplicationInterval),
                        Duration.ofMillis(deduplicationInterval),
                        false
                ),
                Serdes.String(), Serdes.String()
        );
        streamsBuilder.addStateStore(dedupStoreBuilder);
    }

    private void registerAggregationStore(StreamsBuilder streamsBuilder) {
        StoreBuilder<KeyValueStore<String, WeatherPacket>> accumStoreBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(AnalysisConstants.ZERO_LOSS_ACCUMULATION_STORE),
                Serdes.String(), weatherPacketSerde
        );
        streamsBuilder.addStateStore(accumStoreBuilder);
    }

    private @NonNull KStream<String, WeatherPacket> runTransactionalDeduplicationPipeline(StreamsBuilder streamsBuilder) {
        KStream<String, WeatherPacket> rawInputStream = streamsBuilder.stream(
                kafkaIngestionLiveTopic,
                Consumed.with(Serdes.String(), weatherPacketSerde)
        );
        KStream<String, WeatherPacket> deduplicatedStream = rawInputStream.process(
                () -> new TelemetryDeduplicationProcessor(deduplicationInterval),
                AnalysisConstants.DEDUPLICATE_ROCKS_DB
        );
        deduplicatedStream.to(
                kafkaAnalysisRawTopic,
                Produced.with(Serdes.String(), weatherPacketSerde)
        );
        return deduplicatedStream;
    }

    private void runTransactionalAggregationStream(KStream<String, WeatherPacket> upstreamStream,
                                                   Map<String, String> serdeConfig) {
        KStream<String, WeatherPacket> repartitionedByLocationStream = upstreamStream.selectKey((key, packet) -> {
            Location location = packet.getLocation();
            double latGrid = clampLatitude(location.getLatitude());
            double lonGrid = clampLongitude(location.getLongitude());
            return latGrid+HASHTAG_DELIMITER+lonGrid;
        }).repartition(Repartitioned.with(Serdes.String(), weatherPacketSerde).withName("spatial-repartition-stream"));
        KStream<String, WeatherMap> historyStream = repartitionedByLocationStream.process(
                () -> new TelemetryAggregationProcessor(persistentService, aggregationSecondsPerInterval),
                AnalysisConstants.ZERO_LOSS_ACCUMULATION_STORE
        );
        Serde<WeatherMap> weatherMapSerde = new KafkaProtobufSerde<>(WeatherMap.class);
        weatherMapSerde.configure(serdeConfig, false);
        historyStream.to(
                kafkaAnalysisHistoryTopic,
                Produced.with(Serdes.String(), weatherMapSerde)
        );
    }
}