package me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.messaging.kafka;

import static me.neobliz1.ecomonitoring.platform.analysis.domain.model.AnalysisConstants.DEDUPLICATE_ROCKS_DB;
import static me.neobliz1.ecomonitoring.platform.analysis.domain.model.AnalysisConstants.ZERO_LOSS_ACCUMULATION_STORE;
import static me.neobliz1.ecomonitoring.platform.analysis.domain.service.AnalysisUtils.clampLatitude;
import static me.neobliz1.ecomonitoring.platform.analysis.domain.service.AnalysisUtils.clampLongitude;
import static me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.messaging.kafka.processor.util.AggregationUtils.getGeohash;
import static me.neobliz1.ecomonitoring.platform.common.constant.PlatformConstants.SCHEMA_REGISTRY_URL;

import io.confluent.kafka.streams.serdes.protobuf.KafkaProtobufSerde;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import me.neobliz1.ecomonitoring.platform.analysis.domain.port.inbound.TelemetryAnalysisService;
import me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.messaging.kafka.processor.WeatherPacketStreamAggregationProcessor;
import me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.messaging.kafka.processor.WeatherPacketStreamDeduplicationProcessor;
import me.neobliz1.ecomonitoring.platform.analysis.infrastructure.config.AnalysisInfrastructureProperties;
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
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.jspecify.annotations.NonNull;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;

import java.time.Duration;
import java.util.Map;

@Slf4j
@RequiredArgsConstructor
public class TelemetryStreamsTopologyOrchestrator implements TelemetryAnalysisService {

    private final ObjectProvider<WeatherPacketStreamDeduplicationProcessor> deduplicationProcessorProvider;
    private final ObjectProvider<WeatherPacketStreamAggregationProcessor> aggregationProcessorProvider;
    private final AnalysisInfrastructureProperties props;

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
        Long deduplicationInterval = props.getKafka().getStreams().getPipeline().getName().getDeduplicationProcessor().getInterval();
        StoreBuilder<WindowStore<String, String>> dedupStoreBuilder = Stores.windowStoreBuilder(
                Stores.persistentWindowStore(
                        DEDUPLICATE_ROCKS_DB,
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
                Stores.persistentKeyValueStore(ZERO_LOSS_ACCUMULATION_STORE),
                Serdes.String(), weatherPacketSerde
        );
        streamsBuilder.addStateStore(accumStoreBuilder);
    }

    private @NonNull KStream<String, WeatherPacket> runTransactionalDeduplicationPipeline(StreamsBuilder streamsBuilder) {
        val topic = props.getKafka().getTopic();
        String kafkaIngestionLiveTopic = topic.getWeatherLive();
        KStream<String, WeatherPacket> rawInputStream = streamsBuilder.stream(
                kafkaIngestionLiveTopic,
                Consumed.with(Serdes.String(), weatherPacketSerde)
        );
        KStream<String, WeatherPacket> deduplicatedStream = rawInputStream.process(
                deduplicationProcessorProvider::getObject,
                DEDUPLICATE_ROCKS_DB
        );
        String kafkaAnalysisRawTopic = topic.getWeatherRaw();
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
            return getGeohash(latGrid, lonGrid);
        }).repartition(Repartitioned.with(Serdes.String(), weatherPacketSerde).withName("spatial-repartition-stream"));
        KStream<String, WeatherMap> historyStream = repartitionedByLocationStream.process(
                getStringWeatherPacketStringWeatherMapProcessorSupplier(),
                ZERO_LOSS_ACCUMULATION_STORE
        );
        Serde<WeatherMap> weatherMapSerde = new KafkaProtobufSerde<>(WeatherMap.class);
        weatherMapSerde.configure(serdeConfig, false);
        val kafkaAnalysisHistoryTopic = props.getKafka().getTopic().getWeatherHistory();
        historyStream.to(
                kafkaAnalysisHistoryTopic,
                Produced.with(Serdes.String(), weatherMapSerde)
        );
    }

    private @NonNull ProcessorSupplier<String, WeatherPacket, String, WeatherMap> getStringWeatherPacketStringWeatherMapProcessorSupplier() {
        return this::getAggregationProcessorPrototypeWithSelfInjection;
    }

    private @NonNull WeatherPacketStreamAggregationProcessor getAggregationProcessorPrototypeWithSelfInjection() {
        WeatherPacketStreamAggregationProcessor processorProxy = aggregationProcessorProvider.getObject();
        processorProxy.setSelf(processorProxy);
        return processorProxy;
    }
}