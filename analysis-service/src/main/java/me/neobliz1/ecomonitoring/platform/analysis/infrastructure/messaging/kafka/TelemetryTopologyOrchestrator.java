package me.neobliz1.ecomonitoring.platform.analysis.infrastructure.messaging.kafka;

import static me.neobliz1.ecomonitoring.platform.common.constant.PlatformConstants.SCHEMA_REGISTRY_URL;

import io.confluent.kafka.streams.serdes.protobuf.KafkaProtobufSerde;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import me.neobliz1.ecomonitoring.platform.analysis.domain.model.AnalysisConstants;
import me.neobliz1.ecomonitoring.platform.analysis.domain.service.TelemetryAnalysisService;
import me.neobliz1.ecomonitoring.platform.analysis.domain.service.TelemetryPersistentService;
import me.neobliz1.ecomonitoring.platform.common.constant.PlatformConstants;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.Location;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.WeatherPacket;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.WeatherMap;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
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

    private static void registerTransactionalStateStores(StreamsBuilder streamsBuilder, Serde<WeatherPacket> weatherPacketSerde) {
        // Local Deduplication Window Store Builder for Pipeline 1
        StoreBuilder<WindowStore<String, String>> dedupStoreBuilder = Stores.windowStoreBuilder(
                Stores.persistentWindowStore(
                        AnalysisConstants.DEDUPLICATE_ROCKS_DB,
                        Duration.ofMinutes(10),
                        Duration.ofMinutes(10),
                        false
                ),
                Serdes.String(), Serdes.String()
        );
        // Local Accumulation Key-Value Store Builder for Pipeline 2
        StoreBuilder<KeyValueStore<String, WeatherPacket>> accumStoreBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(AnalysisConstants.ZERO_LOSS_ACCUMULATION_STORE),
                Serdes.String(), weatherPacketSerde
        );
        streamsBuilder.addStateStore(dedupStoreBuilder);
        streamsBuilder.addStateStore(accumStoreBuilder);
    }

    @Override
    public KStream<String, WeatherPacket> buildTopology(StreamsBuilder streamsBuilder) {
        Map<String, String> serdeConfig = Map.of(SCHEMA_REGISTRY_URL, schemaRegistryUrl);
        Serde<WeatherPacket> weatherPacketSerde = new KafkaProtobufSerde<>(WeatherPacket.class);
        weatherPacketSerde.configure(serdeConfig, false);
        registerTransactionalStateStores(streamsBuilder, weatherPacketSerde);
        KStream<String, WeatherPacket> deduplicatedStream = runTransactionalDeduplicationPipeline(streamsBuilder, weatherPacketSerde);
        runTransactionalAggregationStream(streamsBuilder, weatherPacketSerde, serdeConfig);
        return deduplicatedStream;
    }

    private @NonNull KStream<String, WeatherPacket> runTransactionalDeduplicationPipeline(StreamsBuilder streamsBuilder,
                                                                                          Serde<WeatherPacket> weatherPacketSerde) {
        KStream<String, WeatherPacket> rawInputStream = streamsBuilder.stream(
                kafkaIngestionLiveTopic,
                Consumed.with(Serdes.String(), weatherPacketSerde)
        );
        // Execute real-time message deduplication checks
        KStream<String, WeatherPacket> deduplicatedStream = rawInputStream.process(
                () -> new TelemetryDeduplicationProcessor(deduplicationInterval),
                AnalysisConstants.DEDUPLICATE_ROCKS_DB
        );
        KStream<String, WeatherPacket> reKeyedStream = deduplicatedStream.selectKey((key, packet) -> {
            Location location = packet.getLocation();
            double latGrid = Math.round(location.getLatitude()*10.0)/10.0;
            double lonGrid = Math.round(location.getLongitude()*10.0)/10.0;
            return latGrid+PlatformConstants.HASHTAG_DELIMITER+lonGrid;
        });
        // Publish to the raw topic. Kafka automatically funnels matching locations to the SAME partition.
        reKeyedStream.to(
                kafkaAnalysisRawTopic,
                Produced.with(Serdes.String(), weatherPacketSerde)
        );
        return deduplicatedStream;
    }

    private void runTransactionalAggregationStream(StreamsBuilder streamsBuilder, Serde<WeatherPacket> weatherPacketSerde,
                                                   Map<String, String> serdeConfig) {
        // Consume from the raw analysis topic where records have been co-partitioned by location
        KStream<String, WeatherPacket> cleanInputStream = streamsBuilder.stream(
                kafkaAnalysisRawTopic,
                Consumed.with(Serdes.String(), weatherPacketSerde)
        );
        // Process records. All stations matching a spatial coordinate hit this EXACT local store.
        KStream<String, WeatherMap> historyStream = cleanInputStream.process(
                () -> new TelemetryAggregationProcessor(persistentService, aggregationSecondsPerInterval),
                AnalysisConstants.ZERO_LOSS_ACCUMULATION_STORE
        );
        Serde<WeatherMap> weatherMapSerde = new KafkaProtobufSerde<>(WeatherMap.class);
        weatherMapSerde.configure(serdeConfig, false);
        // Stream the final integrated ten-minutes WeatherMaps out to the history topic
        historyStream.to(
                kafkaAnalysisHistoryTopic,
                Produced.with(Serdes.String(), weatherMapSerde)
        );
    }
}