package me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.support;

import static me.neobliz1.ecomonitoring.platform.common.constant.PlatformConstants.SCHEMA_REGISTRY_URL;
import static me.neobliz1.ecomonitoring.platform.test.common.util.WeatherTestUtils.getConsumerConf;
import static me.neobliz1.ecomonitoring.platform.test.common.util.WeatherTestUtils.getProducerConf;
import static me.neobliz1.ecomonitoring.platform.test.common.util.WeatherTestUtils.getTestKafkaAdminConf;
import static me.neobliz1.ecomonitoring.platform.test.common.util.WeatherTestUtils.loadEnvironmentMap;

import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import lombok.extern.slf4j.Slf4j;
import me.neobliz1.ecomonitoring.platform.analysis.AnalysisBootEngine;
import me.neobliz1.ecomonitoring.platform.analysis.domain.port.inbound.TelemetryQueryService;
import me.neobliz1.ecomonitoring.platform.analysis.domain.service.TelemetryUtils;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.AirQualityReading;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.AmbientReading;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.Location;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.OpticalReading;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.PrecipitationReading;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.SensorReading;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.WeatherPacket;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.WindReading;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.WeatherMap;
import me.neobliz1.ecomonitoring.platform.test.common.listener.TestKafkaListener;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;
import org.springframework.util.FileSystemUtils;
import org.testcontainers.containers.ComposeContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.File;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
@Testcontainers
@ActiveProfiles({ "dev", "common", "local" })
@SpringBootTest(classes = AnalysisBootEngine.class)
@TestPropertySource(locations = "classpath:.env.test")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
public abstract class IntegrationTestSupport extends AssertionTestSupport {

    public static final float FLUSH_PACKET_TEMPERATURE = 20.0f;
    public static final float FLUSH_PACKET_HUMIDITY = 45.0f;
    public static final float FLUSH_PACKET_PRESSURE = 1013.25f;
    public static final double FLUSH_LAT = 0.0;
    static final long BUCKET_SIZE_MS = 660_000L; // 11 minutes

    @DynamicPropertySource
    static void dynamicPropertySet(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.streams.application-id",
                () -> "eco-analysis-topology-test-"+java.util.UUID.randomUUID());
    }

    @Autowired
    private KafkaProperties kafkaProperties;
    TestKafkaListener<WeatherMap> kafkaHistoryListener;
    @Autowired
    private StreamsBuilderFactoryBean streamsBuilderFactoryBean;
    @Autowired
    protected RedisTemplate<String, byte[]> protobufRedisTemplate;
    @Autowired
    protected StringRedisTemplate redisTemplate;
    @Autowired
    protected TelemetryQueryService telemetryQueryService;

    @Value("${spring.kafka.topic.weather-live}")
    protected String kafkaIngestionTopic;
    @Value("${spring.kafka.topic.weather-raw}")
    String kafkaAnalysisRawTopic;
    @Value("${spring.kafka.topic.weather-history}")
    String kafkaAnalysisHistoryTopic;
    @Value("${spring.kafka.streams.properties.schema.registry.url}")
    private String schemaRegistryUrl;
    @Value("${spring.kafka.streams.pipeline.name.aggregation-processor.interval}")
    private Integer aggregationSecondsPerInterval;

    protected Producer<String, WeatherPacket> testProducer;
    protected Consumer<String, WeatherPacket> rawTopicConsumer;
    Consumer<String, WeatherMap> historyTopicConsumer;

    @BeforeEach
    public void setupEcosystem() {
        setupKafkaProducer();
        setupConsumersWithRebalanceListeners();
        kafkaHistoryListener = new TestKafkaListener<>(historyTopicConsumer);
    }

    @AfterEach
    public void teardownEcosystem() throws ExecutionException, InterruptedException {
        clearKafkaTopics();
        clearRocksDbSt();
        clearRedisCache();
        if(testProducer!=null) testProducer.close();
        if(rawTopicConsumer!=null) rawTopicConsumer.close();
        kafkaHistoryListener.close();
    }

    private void setupKafkaProducer() {
        String bootstrapServersCsv = String.join(",", kafkaProperties.getBootstrapServers());
        Map<String, Object> producerProps = getProducerConf("client", "client-secret-pass", bootstrapServersCsv, schemaRegistryUrl);
        testProducer = new KafkaProducer<>(producerProps);
    }

    private void setupConsumersWithRebalanceListeners() {
        String bootstrapServersCsv = String.join(",", kafkaProperties.getBootstrapServers());
        String executionRunId = String.valueOf(Instant.now().toEpochMilli());
        Map<String, Object> baseConsumerProps = createBaseConsumerProps(bootstrapServersCsv, schemaRegistryUrl);

        rawTopicConsumer = createRawConsumer(baseConsumerProps, executionRunId);
        historyTopicConsumer = createHistoryConsumer(baseConsumerProps, executionRunId);
    }

    private Map<String, Object> createBaseConsumerProps(String bootstrapServersCsv, String schemaRegistryUrl) {
        Map<String, Object> baseConsumerProps = new HashMap<>(
                getConsumerConf("client", "client-secret-pass", bootstrapServersCsv, schemaRegistryUrl));
        baseConsumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_uncommitted");
        baseConsumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return baseConsumerProps;
    }

    private Consumer<String, WeatherPacket> createRawConsumer(Map<String, Object> baseConsumerProps, String executionRunId) {
        Map<String, Object> rawConsumerProps = new HashMap<>(baseConsumerProps);
        rawConsumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-raw-group-"+executionRunId);

        Map<String, Object> rawSrConfig = createSchemaRegistryConfig(schemaRegistryUrl, WeatherPacket.class);
        KafkaProtobufDeserializer<WeatherPacket> rawDeserializer = new KafkaProtobufDeserializer<>();
        rawDeserializer.configure(rawSrConfig, false);

        Consumer<String, WeatherPacket> consumer = new KafkaConsumer<>(rawConsumerProps, new StringDeserializer(), rawDeserializer);
        consumer.subscribe(Collections.singletonList(kafkaAnalysisRawTopic), createRebalanceListener(consumer));
        return consumer;
    }

    private Consumer<String, WeatherMap> createHistoryConsumer(Map<String, Object> baseConsumerProps, String executionRunId) {
        Map<String, Object> historyConsumerProps = new HashMap<>(baseConsumerProps);
        historyConsumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-history-group-"+executionRunId);

        Map<String, Object> historySrConfig = createSchemaRegistryConfig(schemaRegistryUrl, WeatherMap.class);
        KafkaProtobufDeserializer<WeatherMap> historyDeserializer = new KafkaProtobufDeserializer<>();
        historyDeserializer.configure(historySrConfig, false);

        Consumer<String, WeatherMap> consumer = new KafkaConsumer<>(historyConsumerProps, new StringDeserializer(), historyDeserializer);
        consumer.subscribe(Collections.singletonList(kafkaAnalysisHistoryTopic), createRebalanceListener(consumer));
        return consumer;
    }

    private <T> Map<String, Object> createSchemaRegistryConfig(String schemaRegistryUrl, Class<T> valueClass) {
        Map<String, Object> config = new HashMap<>();
        config.put(SCHEMA_REGISTRY_URL, schemaRegistryUrl);
        config.put("specific.protobuf.value.type", valueClass);
        return config;
    }

    private ConsumerRebalanceListener createRebalanceListener(Object consumer) {
        return new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                if(consumer instanceof Consumer) {
                    ((Consumer<?, ?>) consumer).seekToBeginning(partitions);
                }
            }
        };
    }

    private void clearRocksDbSt() {
        if(streamsBuilderFactoryBean!=null) {
            streamsBuilderFactoryBean.stop();
            File rocksDbDirectory = new File("/tmp/kafka-streams/analysis-state");
            FileSystemUtils.deleteRecursively(rocksDbDirectory);
            streamsBuilderFactoryBean.start();
        }
    }

    private void clearRedisCache() {
        if(protobufRedisTemplate!=null && protobufRedisTemplate.getConnectionFactory()!=null) {
            protobufRedisTemplate.execute((RedisConnection connection) -> {
                connection.serverCommands().flushAll();
                return null;
            });
        }
    }

    @SuppressWarnings("resource")
    protected static ComposeContainer getComposeContainer() {
        return new ComposeContainer(new File("../docker/analysis-test-docker-compose.yaml"))
                .withEnv(loadEnvironmentMap())
                .withRemoveVolumes(true)
                .withTailChildContainers(true);
    }

    protected long getCurrentBucketFloor() {
        return TelemetryUtils.getAggregationBucketFloorInterval(Instant.now().toEpochMilli(), aggregationSecondsPerInterval);
    }

    protected long getNextBucketFloor(long timestamp) {
        return TelemetryUtils.getAggregationBucketFloorInterval(timestamp+BUCKET_SIZE_MS, aggregationSecondsPerInterval);
    }

    protected String calculateGridCellKey(double latitude, double longitude) {
        return String.format("%.1f#%.1f",
                Math.round(latitude*10.0)/10.0,
                Math.round(longitude*10.0)/10.0);
    }

    protected double[][] getEdgeCaseCoordinates() {
        return new double[][]{
                { -90.0, FLUSH_LAT },
                { 90.0, FLUSH_LAT },
                { FLUSH_LAT, -180.0 },
                { FLUSH_LAT, 180.0 },
                { -90.0, -180.0 },
                { 90.0, 180.0 },
        };
    }

    protected WeatherPacket createBasicPacket(String stationId, long timestamp, double lat, double lon) {
        return WeatherPacket.newBuilder()
                .setStationId(stationId)
                .setTimestamp(timestamp)
                .setLocation(Location.newBuilder()
                        .setLatitude(lat)
                        .setLongitude(lon)
                        .build())
                .addReadings(SensorReading.newBuilder()
                        .setAmbient(AmbientReading.newBuilder()
                                .setTemperatureC(FLUSH_PACKET_TEMPERATURE)
                                .setHumidityPct(FLUSH_PACKET_HUMIDITY)
                                .setPressureHpa(FLUSH_PACKET_PRESSURE)
                                .build())
                        .build())
                .build();
    }

    protected WeatherPacket createPacketWithAmbientReadings(String stationId, long timestamp,
                                                            double lat, double lon, float temperatureC, float humidityPct, float pressureHpa) {
        return WeatherPacket.newBuilder()
                .setStationId(stationId)
                .setTimestamp(timestamp)
                .setLocation(Location.newBuilder()
                        .setLatitude(lat)
                        .setLongitude(lon)
                        .build())
                .addReadings(SensorReading.newBuilder()
                        .setAmbient(AmbientReading.newBuilder()
                                .setTemperatureC(temperatureC)
                                .setHumidityPct(humidityPct)
                                .setPressureHpa(pressureHpa)
                                .build())
                        .build())
                .build();
    }

    protected WeatherPacket createPacketWithWindReadings(String stationId, long timestamp,
                                                         double lat, double lon, float speedMps, int directionDeg, float gustMps) {
        return WeatherPacket.newBuilder()
                .setStationId(stationId)
                .setTimestamp(timestamp)
                .setLocation(Location.newBuilder()
                        .setLatitude(lat)
                        .setLongitude(lon)
                        .build())
                .addReadings(SensorReading.newBuilder()
                        .setWind(WindReading.newBuilder()
                                .setSpeedMps(speedMps)
                                .setDirectionDeg(directionDeg)
                                .setGustMps(gustMps)
                                .build())
                        .build())
                .build();
    }

    protected WeatherPacket createPacketWithAirQualityReadings(String stationId, long timestamp,
                                                               double lat, double lon, float pm100, float pm25, float pm10, float vocIndex, float noiseDb) {
        return WeatherPacket.newBuilder()
                .setStationId(stationId)
                .setTimestamp(timestamp)
                .setLocation(Location.newBuilder()
                        .setLatitude(lat)
                        .setLongitude(lon)
                        .build())
                .addReadings(SensorReading.newBuilder()
                        .setAirQuality(AirQualityReading.newBuilder()
                                .setPm100(pm100)
                                .setPm25(pm25)
                                .setPm10(pm10)
                                .setVocIndex(vocIndex)
                                .setNoiseDb(noiseDb)
                                .build())
                        .build())
                .build();
    }

    protected WeatherPacket createPacketWithPrecipitationReadings(String stationId, long timestamp,
                                                                  double lat, double lon, float rainRateMmH, float snowDepthCm, float evaporationRate) {
        return WeatherPacket.newBuilder()
                .setStationId(stationId)
                .setTimestamp(timestamp)
                .setLocation(Location.newBuilder()
                        .setLatitude(lat)
                        .setLongitude(lon)
                        .build())
                .addReadings(SensorReading.newBuilder()
                        .setPrecipitation(PrecipitationReading.newBuilder()
                                .setRainRateMmH(rainRateMmH)
                                .setSnowDepthCm(snowDepthCm)
                                .setEvaporationRate(evaporationRate)
                                .build())
                        .build())
                .build();
    }

    protected WeatherPacket createPacketWithOpticalReadings(String stationId, long timestamp,
                                                            double lat, double lon, float uvIndex, float solarRadiationWm2, float lux, float visibilityM) {
        return WeatherPacket.newBuilder()
                .setStationId(stationId)
                .setTimestamp(timestamp)
                .setLocation(Location.newBuilder()
                        .setLatitude(lat)
                        .setLongitude(lon)
                        .build())
                .addReadings(SensorReading.newBuilder()
                        .setOptical(OpticalReading.newBuilder()
                                .setUvIndex(uvIndex)
                                .setSolarRadiationWm2(solarRadiationWm2)
                                .setLux(lux)
                                .setVisibilityM(visibilityM)
                                .build())
                        .build())
                .build();
    }

    protected WeatherPacket createFullSensorPacket(String stationId, long timestamp, double lat, double lon) {
        return WeatherPacket.newBuilder()
                .setStationId(stationId)
                .setTimestamp(timestamp)
                .setLocation(Location.newBuilder()
                        .setLatitude(lat)
                        .setLongitude(lon)
                        .build())
                .addReadings(SensorReading.newBuilder()
                        .setAmbient(AmbientReading.newBuilder()
                                .setTemperatureC(22.5f)
                                .setHumidityPct(55.0f)
                                .setPressureHpa(FLUSH_PACKET_PRESSURE)
                                .setLeafWetnessPct(30.0f)
                                .build())
                        .build())
                .addReadings(SensorReading.newBuilder()
                        .setWind(WindReading.newBuilder()
                                .setSpeedMps(5.5f)
                                .setDirectionDeg(180)
                                .setGustMps(8.0f)
                                .build())
                        .build())
                .addReadings(SensorReading.newBuilder()
                        .setAirQuality(AirQualityReading.newBuilder()
                                .setPm100(50.0f)
                                .setPm25(25.0f)
                                .setPm10(10.0f)
                                .setVocIndex(0.5f)
                                .setNoiseDb(FLUSH_PACKET_HUMIDITY)
                                .build())
                        .build())
                .addReadings(SensorReading.newBuilder()
                        .setPrecipitation(PrecipitationReading.newBuilder()
                                .setRainRateMmH(2.5f)
                                .setSnowDepthCm(0.0f)
                                .setEvaporationRate(1.0f)
                                .build())
                        .build())
                .addReadings(SensorReading.newBuilder()
                        .setOptical(OpticalReading.newBuilder()
                                .setUvIndex(5.0f)
                                .setSolarRadiationWm2(800.0f)
                                .setLux(50000.0f)
                                .setVisibilityM(10.0f)
                                .build())
                        .build())
                .build();
    }

    protected void sendPacket(WeatherPacket packet) throws Exception {
        testProducer.send(new ProducerRecord<>(
                kafkaIngestionTopic,
                0,
                packet.getTimestamp(),
                packet.getStationId(),
                packet
        )).get();
    }

    protected void sendPackets(List<WeatherPacket> packets, long delayMs) throws Exception {
        for(WeatherPacket packet : packets) {
            sendPacket(packet);
            if(delayMs>0) {
                Thread.sleep(delayMs);
            }
        }
        testProducer.flush();
    }

    protected List<WeatherPacket> generateMockPackets(int count, String stationId, long startTimeFloor, double lat, double lon) {
        List<WeatherPacket> packets = new ArrayList<>(count);
        for(int i = 0; i<count; i++) {
            WeatherPacket packet = WeatherPacket.newBuilder()
                    .setStationId(stationId)
                    .setTimestamp(startTimeFloor+(i*50L))
                    .setLocation(Location.newBuilder().setLatitude(lat).setLongitude(lon).build())
                    .addReadings(SensorReading.newBuilder()
                            .setAmbient(AmbientReading.newBuilder()
                                    .setTemperatureC(FLUSH_PACKET_TEMPERATURE+(i*0.1f))
                                    .setHumidityPct(FLUSH_PACKET_HUMIDITY)
                                    .setPressureHpa(1013.2f)
                                    .build())
                            .build())
                    .build();
            packets.add(packet);
        }
        return packets;
    }

    protected List<WeatherPacket> generateMockPacketsWithKnownValues(String stationId,
                                                                     long startTimeFloor, double lat, double lon) {
        List<WeatherPacket> packets = new ArrayList<>(10);
        for(int i = 0; i<10; i++) {
            WeatherPacket packet = WeatherPacket.newBuilder()
                    .setStationId(stationId)
                    .setTimestamp(startTimeFloor+(i*100L))
                    .setLocation(Location.newBuilder().setLatitude(lat).setLongitude(lon).build())
                    .addReadings(SensorReading.newBuilder()
                            .setAmbient(AmbientReading.newBuilder()
                                    .setTemperatureC((float) 20.0+i)
                                    .setHumidityPct((float) 45.0)
                                    .setPressureHpa((float) 1013.25)
                                    .build())
                            .build())
                    .build();
            packets.add(packet);
        }
        return packets;
    }

    protected <T> ConsumerRecord<String, T> pollSingleRecord(Consumer<String, T> consumer) {
        ConsumerRecords<String, T> records = consumer.poll(Duration.ofSeconds(40));
        if(records.isEmpty()) {
            return null;
        }
        return records.iterator().next();
    }

    protected List<WeatherMap> collectHistoryRecords(int targetRecordsNum) {
        AtomicReference<List<WeatherMap>> matchedMap = new AtomicReference<>();
        Awaitility.await()
                .atMost(Duration.ofMinutes(1))
                .pollInterval(Duration.ofMillis(200))
                .until(() -> {
                    List<WeatherMap> weatherMaps = new ArrayList<>(kafkaHistoryListener.getReceivedPackets());
                    if(weatherMaps.size()>=targetRecordsNum) {
                        matchedMap.set(weatherMaps);
                        return true;
                    } else return false;
                });
        return matchedMap.get();
    }

    protected List<WeatherPacket> collectRawRecords() {
        List<WeatherPacket> results = new ArrayList<>();
        for(int i = 0; i<3; i++) {
            ConsumerRecord<String, WeatherPacket> record = pollSingleRecord(rawTopicConsumer);
            if(record!=null) {
                results.add(record.value());
            } else {
                break;
            }
        }
        return results;
    }

    void clearKafkaTopics() throws InterruptedException, ExecutionException {
        List<String> bootstrapServersList = kafkaProperties.getBootstrapServers();
        String bootstrapServersCsv = String.join(",", bootstrapServersList);
        Map<String, Object> adminConf = getTestKafkaAdminConf("admin", "admin-password", bootstrapServersCsv);

        try(AdminClient adminClient = AdminClient.create(adminConf)) {
            List<String> topicsToClear = List.of(kafkaIngestionTopic, kafkaAnalysisRawTopic, kafkaAnalysisHistoryTopic);
            adminClient.deleteTopics(topicsToClear).all().get();
            Thread.sleep(300);
            NewTopic ingestionTopic = new NewTopic(kafkaIngestionTopic, 6, (short) 3)
                    .configs(Map.of(
                            TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2",
                            TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE,
                            TopicConfig.RETENTION_MS_CONFIG, "604800000" // 7 days
                    ));
            NewTopic rawTopic = new NewTopic(kafkaAnalysisRawTopic, 6, (short) 3)
                    .configs(Map.of(
                            TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2",
                            TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE,
                            TopicConfig.RETENTION_MS_CONFIG, "86400000" // 1 day
                    ));
            NewTopic historyTopic = new NewTopic(kafkaAnalysisHistoryTopic, 6, (short) 3)
                    .configs(Map.of(
                            TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2",
                            TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT,
                            TopicConfig.RETENTION_MS_CONFIG, "-1" // Infinite retention
                    ));
            adminClient.createTopics(List.of(ingestionTopic, rawTopic, historyTopic)).all().get();
            Thread.sleep(200);
        }
    }

    protected void sendFlushPackage(String stationId, long currentBucketFloor, double lat, double lon) throws Exception {
        WeatherPacket basicPacket = createBasicPacket(stationId, currentBucketFloor+660000, lat, lon);
        sendPacket(basicPacket);
        testProducer.flush();
    }

    protected void sendWarmupPackage(long currentBucketFloor) throws Exception {
        WeatherPacket basicPacket = createBasicPacket("1", currentBucketFloor, FLUSH_LAT, FLUSH_LAT);
        sendPacket(basicPacket);
        testProducer.flush();
    }

    public WeatherMap findWeatherMapByGridCellAnBucketFloor(String gridCellKey, long currentBucketFloor) {
        AtomicReference<WeatherMap> matchedMap = new AtomicReference<>();

        Awaitility.await()
                .atMost(Duration.ofMinutes(1))
                .pollInterval(Duration.ofMillis(200))
                .until(() -> {
                    List<WeatherMap> currentPackets = new ArrayList<>(kafkaHistoryListener.getReceivedPackets());
                    return currentPackets.stream()
                            .filter(map -> map.getGridCellsMap().keySet().stream()
                                    .anyMatch(k -> k.contains(gridCellKey)
                                            && k.contains(String.valueOf(currentBucketFloor))))
                            .findFirst()
                            .map(map -> {
                                matchedMap.set(map);
                                return true;
                            })
                            .orElse(false);
                });

        return matchedMap.get();
    }
}