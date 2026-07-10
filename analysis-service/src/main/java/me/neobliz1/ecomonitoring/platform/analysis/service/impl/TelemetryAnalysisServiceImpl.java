package me.neobliz1.ecomonitoring.platform.analysis.service.impl;

import static me.neobliz1.ecomonitoring.platform.analysis.processor.TelemetryAggregationProcessor.ZERO_LOSS_ACCUMULATION_STORE;

import com.google.protobuf.util.JsonFormat;
import io.confluent.kafka.streams.serdes.protobuf.KafkaProtobufSerde;
import lombok.RequiredArgsConstructor;
import me.neobliz1.ecomonitoring.platform.analysis.processor.TelemetryAggregationProcessor;
import me.neobliz1.ecomonitoring.platform.analysis.processor.TelemetryDeduplicationProcessor;
import me.neobliz1.ecomonitoring.platform.analysis.service.TelemetryAnalysisService;
import me.neobliz1.ecomonitoring.platform.model.exception.ProtocolBufferTranslationException;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.AirQualityReading;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.AmbientReading;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.OpticalReading;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.PrecipitationReading;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.SensorReading;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.WeatherPacket;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.WindReading;
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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@RequiredArgsConstructor
public class TelemetryAnalysisServiceImpl implements TelemetryAnalysisService {

    private static final long BUCKET_INTERVAL_MS = 300_000L;
    public static final String WEATHER_MAP_KEY = "weather:map:";

    private final StringRedisTemplate redisTemplate;

    @Value("${spring.kafka.topic.weather-live}")
    private String kafkaIngestionLiveTopic;
    @Value("${spring.kafka.topic.weather-raw}")
    private String kafkaAnalysisRawTopic;
    @Value("${spring.kafka.topic.weather-history}")
    private String kafkaAnalysisHistoryTopic;
    @Value("${spring.kafka.streams.pipeline.name.aggregation-processor.interval}")
    private Integer interval;
    @Value("${spring.kafka.streams.pipeline.name.deduplication-processor.interval}")
    private Long deduplicationInterval;
    @Value("${spring.kafka.streams.properties.schema.registry.url}")
    private String schemaRegistryUrl;

    @Override
    public KStream<String, WeatherPacket> buildTopology(StreamsBuilder streamsBuilder) {
        Map<String, String> serdeConfig = Map.of(
                "schema.registry.url", schemaRegistryUrl
        );
        Serde<WeatherPacket> weatherPacketSerde = new KafkaProtobufSerde<>(WeatherPacket.class);
        weatherPacketSerde.configure(serdeConfig, false);
        // Register State Stores globally to the Topology
        StoreBuilder<WindowStore<String, String>> dedupStoreBuilder = Stores.windowStoreBuilder(
                Stores.persistentWindowStore(
                        TelemetryDeduplicationProcessor.DEDUPLICATE_ROCKS_DB,
                        Duration.ofMinutes(10),
                        Duration.ofMinutes(10),
                        false
                ),
                Serdes.String(), Serdes.String()
        );
        StoreBuilder<KeyValueStore<String, WeatherPacket>> accumStoreBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(ZERO_LOSS_ACCUMULATION_STORE),
                Serdes.String(), weatherPacketSerde);
        streamsBuilder.addStateStore(dedupStoreBuilder);
        streamsBuilder.addStateStore(accumStoreBuilder);
        // ==========================================
        // PIPELINE 1: Deduplication Pipeline Layout
        // ==========================================
        KStream<String, WeatherPacket> rawInputStream = streamsBuilder.stream(
                kafkaIngestionLiveTopic,
                Consumed.with(Serdes.String(), weatherPacketSerde)
        );
        KStream<String, WeatherPacket> deduplicatedStream = rawInputStream.process(
                () -> new TelemetryDeduplicationProcessor(deduplicationInterval),
                TelemetryDeduplicationProcessor.DEDUPLICATE_ROCKS_DB
        );
        deduplicatedStream.to(
                kafkaAnalysisRawTopic,
                Produced.with(Serdes.String(), weatherPacketSerde)
        );
        // ==========================================
        // PIPELINE 2: Aggregation Pipeline Layout
        // ==========================================
        KStream<String, WeatherPacket> cleanInputStream = streamsBuilder.stream(
                kafkaAnalysisRawTopic,
                Consumed.with(Serdes.String(), weatherPacketSerde)
        );
        // This stream reads committed messages, maps to cache views, and builds the history snapshot
        KStream<String, WeatherMap> historyStream = cleanInputStream.process(
                () -> new TelemetryAggregationProcessor(this, interval),
                ZERO_LOSS_ACCUMULATION_STORE
        );
        Serde<WeatherMap> weatherMapSerde = new KafkaProtobufSerde<>(WeatherMap.class);
        weatherMapSerde.configure(serdeConfig, false);
        historyStream.to(
                kafkaAnalysisHistoryTopic,
                Produced.with(Serdes.String(), weatherMapSerde)
        );
        // Return the clean stream to satisfy  application configuration definitions bean
        return deduplicatedStream;
    }

    @Override
    public void updateRealTimeSlidingWindow(WeatherPacket packet, double latGrid, double lonGrid) {
        String redisKey = "weather:hotwindow:"+packet.getStationId();
        String hashField = String.format("%.1f_%.1f", latGrid, lonGrid);

        redisTemplate.opsForHash().put(redisKey, hashField, String.valueOf(packet.getTimestamp()));
        redisTemplate.expire(redisKey, Duration.ofHours(24));
    }

    @Override
    public void persistAggregatedHistory(Map<Long, Map<String, List<WeatherPacket>>> extractionMatrix, ProcessorContext<String, WeatherMap> context,
                                         long currentStreamTime) {
        extractionMatrix.forEach((bucketTime, spatialMap) -> {
            WeatherMap.Builder weatherMapBuilder = WeatherMap.newBuilder()
                    .setTimestampBucket(bucketTime)
                    .setIntervalMinutes(5);

            spatialMap.forEach((spatialKey, packetsList) -> {
                GridCellLayers.Builder cellBuilder = aggregatePackets(packetsList);
                cellBuilder.setGeohash(spatialKey);

                weatherMapBuilder.putGridCells(spatialKey, cellBuilder.build());
            });

            WeatherMap finalReport = weatherMapBuilder.build();
            String transactionRoutingKey = String.valueOf(bucketTime);


            String redisHistoryKey = WEATHER_MAP_KEY+bucketTime;
            finalReport.getGridCellsMap().forEach((geohash, cellLayers) ->
                    redisTemplate.opsForHash().put(redisHistoryKey, geohash, cellLayers.toString()));
            redisTemplate.expire(redisHistoryKey, Duration.ofHours(24));

            Record<String, WeatherMap> record = new Record<>(transactionRoutingKey, finalReport, currentStreamTime);
            context.forward(record);
        });
    }

    @Override
    public Optional<String> getLatestFiveMinuteWeatherMapJson() {
        // 1. Calculate deterministic bucket floor coordinate parameter
        long now = Instant.now().toEpochMilli();
        long activeBucketFloor = now-(now%BUCKET_INTERVAL_MS);
        String redisHistoryKey = WEATHER_MAP_KEY+activeBucketFloor;

        // 2. Synchronously read out the entire hash matrix entries map from Redis
        Map<Object, Object> rawHashDataMatrix = redisTemplate.opsForHash().entries(redisHistoryKey);

        if(rawHashDataMatrix==null || rawHashDataMatrix.isEmpty()) {
            return Optional.empty();
        }

        try {
            // 3. Assemble and inflate the global parent tracking document layout
            WeatherMap.Builder weatherMapBuilder = WeatherMap.newBuilder()
                    .setTimestampBucket(activeBucketFloor)
                    .setIntervalMinutes(5);

            for(Map.Entry<Object, Object> cellEntry : rawHashDataMatrix.entrySet()) {
                String geohashKey = (String) cellEntry.getKey();
                String stringifiedCellData = (String) cellEntry.getValue();

                GridCellLayers.Builder cellBuilder = GridCellLayers.newBuilder();
                JsonFormat.parser().ignoringUnknownFields().merge(stringifiedCellData, cellBuilder);

                weatherMapBuilder.putGridCells(geohashKey, cellBuilder.build());
            }

            // 4. Transform native metric map structures straight to readable JSON format string
            String jsonMapResponse = JsonFormat.printer().alwaysPrintFieldsWithNoPresence()
                    .preservingProtoFieldNames()
                    .print(weatherMapBuilder.build());

            return Optional.of(jsonMapResponse);

        } catch(Exception e) {
            throw new ProtocolBufferTranslationException("Failed to translate underlying Protobuf structural matrices in synchronous chain", e);
        }
    }

    private GridCellLayers.Builder aggregatePackets(List<WeatherPacket> packets) {
        GridCellLayers.Builder cellLayerBuilder = GridCellLayers.newBuilder()
                .setReadingCount(packets.size());

        double tempSum = 0, humSum = 0, pressSum = 0;
        double windSpeedSum = 0, windSinSum = 0, windCosSum = 0;
        double pm100Sum = 0, pm25Sum = 0, pm10Sum = 0;
        double vocSum = 0, noiseSum = 0;
        double rainSum = 0, snowSum = 0, evapSum = 0;
        double uvSum = 0, solarSum = 0, luxSum = 0, visSum = 0;

        int ambientCount = 0, windCount = 0, aqCount = 0, precipCount = 0, optCount = 0;

        for(WeatherPacket packet : packets) {
            for(SensorReading reading : packet.getReadingsList()) {
                switch(reading.getSensorDataCase()) {
                    case AMBIENT:
                        AmbientReading ambient = reading.getAmbient();
                        tempSum += ambient.getTemperatureC();
                        humSum += ambient.getHumidityPct();
                        pressSum += ambient.getPressureHpa();
                        ambientCount++;
                        break;
                    case WIND:
                        WindReading wind = reading.getWind();
                        windSpeedSum += wind.getSpeedMps();
                        double rad = Math.toRadians(wind.getDirectionDeg());
                        windSinSum += Math.sin(rad);
                        windCosSum += Math.cos(rad);
                        windCount++;
                        break;
                    case AIR_QUALITY:
                        AirQualityReading aq = reading.getAirQuality();
                        pm100Sum += aq.getPm100();
                        pm25Sum += aq.getPm25();
                        pm10Sum += aq.getPm10();
                        vocSum += aq.getVocIndex();
                        noiseSum += aq.getNoiseDb();
                        aqCount++;
                        break;
                    case PRECIPITATION:
                        PrecipitationReading precipitation = reading.getPrecipitation();
                        rainSum += precipitation.getRainRateMmH();
                        snowSum += precipitation.getSnowDepthCm();
                        evapSum += precipitation.getEvaporationRate();
                        precipCount++;
                        break;
                    case OPTICAL:
                        OpticalReading opt = reading.getOptical();
                        uvSum += opt.getUvIndex();
                        solarSum += opt.getSolarRadiationWm2();
                        luxSum += opt.getLux();
                        visSum += opt.getVisibilityM();
                        optCount++;
                        break;
                    case SENSORDATA_NOT_SET:
                        break;
                }
            }
        }

        if(ambientCount>0) {
            cellLayerBuilder.setAvgTemperature(tempSum/ambientCount);
            cellLayerBuilder.setAvgHumidity(humSum/ambientCount);
            cellLayerBuilder.setAvgPressure(pressSum/ambientCount);
        }
        if(windCount>0) {
            cellLayerBuilder.setAvgWindSpeed(windSpeedSum/windCount);
            double avgAngleDeg = Math.toDegrees(Math.atan2(windSinSum/windCount, windCosSum/windCount));
            if(avgAngleDeg<0) avgAngleDeg += 360.0;
            cellLayerBuilder.setAvgWindDirection((int) Math.round(avgAngleDeg));
        }
        if(aqCount>0) {
            cellLayerBuilder.setAvgPm100(pm100Sum/aqCount);
            cellLayerBuilder.setAvgPm25(pm25Sum/aqCount);
            cellLayerBuilder.setAvgPm10(pm10Sum/aqCount);
            cellLayerBuilder.setAvgVoc(vocSum/aqCount);
            cellLayerBuilder.setAvgNoiseDb(noiseSum/aqCount);
        }
        if(precipCount>0) {
            cellLayerBuilder.setAvgRainMm(rainSum/precipCount);
            cellLayerBuilder.setAvgSnowCm(snowSum/precipCount);
            cellLayerBuilder.setAvgEvapRate(evapSum/precipCount);
        }
        if(optCount>0) {
            cellLayerBuilder.setAvgUvIndex(uvSum/optCount);
            cellLayerBuilder.setAvgSolarRadiationWm2(solarSum/optCount);
            cellLayerBuilder.setAvgLux(luxSum/optCount);
            cellLayerBuilder.setAvgVisibilityM(visSum/optCount);
        }

        return cellLayerBuilder;
    }
}