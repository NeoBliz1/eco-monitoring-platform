package me.neobliz1.ecomonitoring.platform.analysis.service.impl;

import static me.neobliz1.ecomonitoring.platform.common.constant.PlatformConstants.HASHTAG_DELIMITER;
import static me.neobliz1.ecomonitoring.platform.common.constant.PlatformConstants.SCHEMA_REGISTRY_URL;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.confluent.kafka.streams.serdes.protobuf.KafkaProtobufSerde;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import me.neobliz1.ecomonitoring.platform.analysis.constants.AnalysisConstants;
import me.neobliz1.ecomonitoring.platform.analysis.processor.TelemetryAggregationProcessor;
import me.neobliz1.ecomonitoring.platform.analysis.processor.TelemetryDeduplicationProcessor;
import me.neobliz1.ecomonitoring.platform.analysis.service.TelemetryAnalysisService;
import me.neobliz1.ecomonitoring.platform.analysis.util.TelemetryAnalysisAccumulator;
import me.neobliz1.ecomonitoring.platform.analysis.util.TelemetryUtils;
import me.neobliz1.ecomonitoring.platform.common.constant.PlatformConstants;
import me.neobliz1.ecomonitoring.platform.model.exception.InvalidCoordinatesSquareException;
import me.neobliz1.ecomonitoring.platform.model.exception.ProtocolBufferTranslationException;
import me.neobliz1.ecomonitoring.platform.model.exception.WeatherMapDataNotFoundException;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.Location;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.SensorReading;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.WeatherPacket;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.GridCellLayers;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.WeatherMap;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.jspecify.annotations.NonNull;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Slf4j
@RequiredArgsConstructor
public class TelemetryAnalysisServiceImpl implements TelemetryAnalysisService {

    private final RedisTemplate<String, byte[]> protobufRedisTemplate;
    private final StringRedisTemplate redisTemplate;

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
    @Value("${spring.redis.records.ttl}")
    private Long redisCacheTtlInterval;
    @Value("${spring.kafka.streams.properties.schema.registry.url}")
    private String schemaRegistryUrl;

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
                () -> new TelemetryAggregationProcessor(this, aggregationSecondsPerInterval),
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
    public void updateRealTimeSlidingWindow(WeatherPacket packet, double latGrid, double lonGrid) {
        String redisKey = AnalysisConstants.WEATHER_HOTWINDOW+latGrid+HASHTAG_DELIMITER+lonGrid;
        String hashField = AnalysisConstants.HOT_WINDOW_PREFIX+packet.getStationId();
        redisTemplate.opsForHash().put(redisKey, hashField, String.format(AnalysisConstants.UTC_TIMESTAMP_FORMAT, packet.getTimestamp()));
        redisTemplate.expire(redisKey, Duration.ofHours(redisCacheTtlInterval));
    }

    @Override
    public void persistAggregatedHistory(Map<Long, Map<String, List<WeatherPacket>>> extractionMatrix,
                                         ProcessorContext<String, WeatherMap> context,
                                         long currentStreamTime) {
        extractionMatrix.forEach((bucketTime, spatialMap) -> {
            WeatherMap.Builder weatherMapBuilder = WeatherMap.newBuilder()
                    .setTimestampBucket(bucketTime)
                    .setIntervalMinutes((int) Duration.ofSeconds(aggregationSecondsPerInterval).toMinutes());

            spatialMap.forEach((spatialKey, packetsList) -> {
                GridCellLayers.Builder cellBuilder = aggregatePackets(packetsList);
                cellBuilder.setGeohash(spatialKey);
                weatherMapBuilder.putGridCells(spatialKey, cellBuilder.build());
            });
            String transactionRoutingKey = String.valueOf(bucketTime);
            String redisHistoryKey = AnalysisConstants.WEATHER_MAP_KEY+getAggregationBucketFloorInterval(bucketTime);
            WeatherMap finalReport = weatherMapBuilder.build();
            finalReport.getGridCellsMap().forEach((geohash, cellLayers) -> {
                try {
                    byte[] binaryPayload = cellLayers.toByteArray();
                    protobufRedisTemplate.opsForHash().put(redisHistoryKey, geohash, binaryPayload);
                } catch(Exception e) {
                    throw new ProtocolBufferTranslationException(String.format("Failed to serialize sub-grid metrics for geohash: %s", geohash), e);
                }
            });
            protobufRedisTemplate.expire(redisHistoryKey, Duration.ofHours(redisCacheTtlInterval));
            Record<String, WeatherMap> record = new Record<>(transactionRoutingKey, finalReport, currentStreamTime);
            context.forward(record);
        });
    }

    @Override
    public String getLatestTimeIntervalWeatherMapByCoordinates(long targetTimestamp, List<Double> coordinatesSquare) {
        if(coordinatesSquare==null || coordinatesSquare.size()<4) {
            throw new InvalidCoordinatesSquareException();
        }
        double minLat = coordinatesSquare.get(0);
        double maxLat = coordinatesSquare.get(1);
        double minLon = coordinatesSquare.get(2);
        double maxLon = coordinatesSquare.get(3);
        boolean isSquare = (maxLat>minLat && maxLon>minLon);
        if(!isSquare) throw new InvalidCoordinatesSquareException();
        long activeBucketFloor = getAggregationBucketFloorInterval(targetTimestamp);
        String redisHistoryKey = AnalysisConstants.WEATHER_MAP_KEY+activeBucketFloor;
        Map<Object, Object> rawHashDataMatrix = protobufRedisTemplate.opsForHash().entries(redisHistoryKey);
        if(rawHashDataMatrix==null || rawHashDataMatrix.isEmpty()) {
            throw new WeatherMapDataNotFoundException();
        }
        WeatherMap.Builder weatherMapBuilder = WeatherMap.newBuilder()
                .setTimestampBucket(activeBucketFloor)
                .setIntervalMinutes((int) Duration.ofSeconds(aggregationSecondsPerInterval).toMinutes());
        rawHashDataMatrix.entrySet().stream()
                .filter(cellEntry -> {
                    String[] parts = String.valueOf(cellEntry.getKey()).split(HASHTAG_DELIMITER);
                    double cellLat;
                    double cellLon;
                    try {
                        cellLat = Double.parseDouble(parts[1]);
                        cellLon = Double.parseDouble(parts[2]);
                    } catch(NumberFormatException|IndexOutOfBoundsException e) {
                        throw new ProtocolBufferTranslationException("Corrupted or malformed grid matrix coordinates key in Redis storage: "
                                +Arrays.toString(parts), e);
                    }
                    return cellLat>=minLat && cellLat<=maxLat && cellLon>=minLon && cellLon<=maxLon;
                })
                .forEach(entry -> {
                    try {
                        GridCellLayers gridCellLayers = GridCellLayers.parseFrom((byte[]) entry.getValue());
                        weatherMapBuilder.putGridCells(String.valueOf(entry.getKey()), gridCellLayers);
                    } catch(InvalidProtocolBufferException e) {
                        throw new ProtocolBufferTranslationException(e);
                    }
                });
        if(weatherMapBuilder.getGridCellsMap().isEmpty()) {
            throw new WeatherMapDataNotFoundException();
        }
        String result;
        try {
            result = JsonFormat.printer()
                    .alwaysPrintFieldsWithNoPresence()
                    .preservingProtoFieldNames()
                    .print(weatherMapBuilder.build());
        } catch(Exception e) {
            throw new ProtocolBufferTranslationException(e);
        }
        return result;
    }

    private GridCellLayers.Builder aggregatePackets(List<WeatherPacket> packets) {
        TelemetryAnalysisAccumulator resultContainer = packets.parallelStream()
                .collect(
                        TelemetryAnalysisAccumulator::new,
                        (container, packet) -> {
                            for (SensorReading reading : packet.getReadingsList()) {
                                container.accumulate(reading);
                            }
                        },
                        TelemetryAnalysisAccumulator::merge
                );

        return resultContainer.applyTo(GridCellLayers.newBuilder()
                .setReadingCount(packets.size()));
    }

    public long getAggregationBucketFloorInterval(long packetTimestampInMillis) {
        long aggIntervalMillis = TelemetryUtils.getMillis(aggregationSecondsPerInterval);
        return packetTimestampInMillis/aggIntervalMillis*aggIntervalMillis;
    }
}