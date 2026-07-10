package me.neobliz1.ecomonitoring.platform.analysis;

import static me.neobliz1.ecomonitoring.platform.common.util.WeatherTestUtils.getConsumerConf;

import lombok.extern.slf4j.Slf4j;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.AmbientReading;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.Location;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.SensorReading;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.WeatherPacket;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.WeatherMap;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.awaitility.Awaitility;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@SpringBootTest
@ActiveProfiles("dev")
@TestPropertySource(locations = "classpath:.env.test")
public abstract class BaseKafkaIntegrationTest {

    @Autowired
    private KafkaProperties kafkaProperties;

    @Value("${spring.kafka.topic.weather-live}")
    protected String kafkaIngestionTopic;
    @Value("${spring.kafka.topic.weather-raw}")
    protected String kafkaAnalysisRawTopic;
    @Value("${spring.kafka.topic.weather-history}")
    protected String kafkaAnalysisHistoryTopic;
    @Value("${spring.kafka.properties.sasl.jaas.config}")
    private String saslJaasConfig;
    @Value("${spring.kafka.streams.properties.schema.registry.url}")
    private String schemaRegistryUrl;

    protected Producer<String, WeatherPacket> testProducer;
    protected Consumer<String, WeatherPacket> rawTopicConsumer;
    protected Consumer<String, WeatherMap> historyTopicConsumer;

    @BeforeAll
    static void beforeAll() {
        waitForConsulServicesToBeHealthy();
    }


    @BeforeEach
    public void setupEcosystem() {
        // 1. Join all available brokers so the client can fail over to 9094, 9194, or 9294 seamlessly
        List<String> bootstrapServersList = kafkaProperties.getBootstrapServers();
        String bootstrapServersCsv = String.join(",", bootstrapServersList);
        String executionRunId = String.valueOf(System.currentTimeMillis());

        // 2. Build the Producer configuration mapping properties
        Map<String, Object> producerProps = getProducerProps(bootstrapServersCsv);

        // 3. Build Base Consumer Properties
        Map<String, Object> baseConsumerProps = new HashMap<>(getConsumerConf("client", "client-secret-pass", bootstrapServersCsv));
        baseConsumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_uncommitted");
        baseConsumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        Map<String, Object> rawConsumerProps = new HashMap<>(baseConsumerProps);
        rawConsumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-raw-group-"+executionRunId);

        Map<String, Object> historyConsumerProps = new HashMap<>(baseConsumerProps);
        historyConsumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-history-group-"+executionRunId);

        // 4. Programmatic Deserializers Setup
        Map<String, Object> rawSrConfig = new HashMap<>();
        rawSrConfig.put("schema.registry.url", schemaRegistryUrl);
        rawSrConfig.put("specific.protobuf.value.type", WeatherPacket.class);

        Map<String, Object> historySrConfig = new HashMap<>();
        historySrConfig.put("schema.registry.url", schemaRegistryUrl);
        historySrConfig.put("specific.protobuf.value.type", WeatherMap.class);

        var rawDeserializer = new io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer<WeatherPacket>();
        rawDeserializer.configure(rawSrConfig, false);

        var historyDeserializer = new io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer<WeatherMap>();
        historyDeserializer.configure(historySrConfig, false);

        // 5. Instantiate and Subscribe using Group-Coordinated listeners to handle KRaft routing
        testProducer = new KafkaProducer<>(producerProps);

        rawTopicConsumer = new KafkaConsumer<>(rawConsumerProps, new StringDeserializer(), rawDeserializer);
        rawTopicConsumer.subscribe(Collections.singletonList(kafkaAnalysisRawTopic), new org.apache.kafka.clients.consumer.ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(java.util.Collection<org.apache.kafka.common.TopicPartition> partitions) {
            }

            @Override
            public void onPartitionsAssigned(java.util.Collection<org.apache.kafka.common.TopicPartition> partitions) {
                // Rewinds offsets to zero as soon as the cluster routes assignments over external listeners
                rawTopicConsumer.seekToBeginning(partitions);
            }
        });

        historyTopicConsumer = new KafkaConsumer<>(historyConsumerProps, new StringDeserializer(), historyDeserializer);
        historyTopicConsumer.subscribe(Collections.singletonList(kafkaAnalysisHistoryTopic), new org.apache.kafka.clients.consumer.ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(java.util.Collection<org.apache.kafka.common.TopicPartition> partitions) {
            }

            @Override
            public void onPartitionsAssigned(java.util.Collection<org.apache.kafka.common.TopicPartition> partitions) {
                historyTopicConsumer.seekToBeginning(partitions);
            }
        });
    }

    private @NotNull Map<String, Object> getProducerProps(String bootstrapServersCsv) {
        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServersCsv);
        producerProps.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        producerProps.put(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-512");
        producerProps.put(SaslConfigs.SASL_JAAS_CONFIG, saslJaasConfig);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer");
        producerProps.put("schema.registry.url", schemaRegistryUrl);
        return producerProps;
    }

    @AfterEach
    public void teardownEcosystem() {
        if(testProducer!=null) testProducer.close();
        if(rawTopicConsumer!=null) rawTopicConsumer.close();
        if(historyTopicConsumer!=null) historyTopicConsumer.close();
    }

    /**
     * Strategy factory method generating an array of packets
     */
    protected List<WeatherPacket> generateMockPackets(int count, String stationId, long startTimeFloor, double lat, double lon) {
        List<WeatherPacket> packets = new ArrayList<>(count);
        for(int i = 0; i<count; i++) {
            WeatherPacket packet = WeatherPacket.newBuilder()
                    .setStationId(stationId)
                    // Offset timestamp slightly per packet to simulate a realistic data stream
                    .setTimestamp(startTimeFloor+(i*50L))
                    .setLocation(Location.newBuilder().setLatitude(lat).setLongitude(lon).build())
                    .addReadings(SensorReading.newBuilder()
                            .setAmbient(AmbientReading.newBuilder()
                                    .setTemperatureC(20.0f+(i*0.1f))
                                    .setHumidityPct(45.0f)
                                    .setPressureHpa(1013.2f)
                                    .build())
                            .build())
                    .build();
            packets.add(packet);
        }
        return packets;
    }

    protected <T> ConsumerRecord<String, T> pollSingleRecord(Consumer<String, T> consumer) {
        ConsumerRecords<String, T> records = consumer.poll(Duration.ofSeconds(40));
        return records.isEmpty()?null:records.iterator().next();
    }

    protected static Map<String, String> loadEnvironmentMap() {
        Map<String, String> envMap = new HashMap<>();
        try {
            File envFile = new File("../docker/.env");
            if(envFile.exists()) {
                Files.readAllLines(Paths.get(envFile.toURI())).stream()
                        .map(String::trim)
                        .filter(line -> !line.isEmpty() && !line.startsWith("#"))
                        .forEach(line -> {
                            String[] parts = line.split("=", 2);
                            if(parts.length==2) {
                                envMap.put(parts[0].trim(), parts[1].trim());
                            }
                        });
            } else {
                throw new IllegalArgumentException("Invalid environment file format");
            }
        } catch(IOException e) {
            System.err.println("Warning: Failed to parse local .env file tokens: "+e.getMessage());
        }

        return envMap;
    }

    private static void waitForConsulServicesToBeHealthy() {
        log.info("⏳ Waiting for all specific Consul discovery nodes to pass healthchecks...");

        List<String> requiredServices = List.of(
                "kafka",
                "redis",
                "schema-registry",
                "consul"
        );

        try(HttpClient client = HttpClient.newHttpClient()) {
            Awaitility.await()
                    .atMost(Duration.ofMinutes(5))
                    .pollInterval(Duration.ofSeconds(3))
                    .ignoreExceptions()
                    .until(() -> {
                        for(String service : requiredServices) {
                            HttpRequest request = HttpRequest.newBuilder()
                                    .uri(URI.create("http://localhost:8500/v1/health/service/"+service))
                                    .GET()
                                    .build();

                            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                            String body = response.body().trim();

                            if("[]".equals(body) || body.isEmpty()) {
                                log.info("▶️ Service '{}' is not registered yet...", service);
                                return false;
                            }

                            if(body.contains("\"Status\":\"critical\"") || body.contains("\"Status\":\"warning\"")) {
                                log.info("⚠️ Service '{}' has checks that are failing or warming up...", service);
                                return false;
                            }

                            if(!body.contains("\"Status\":\"passing\"")) {
                                log.info("🔄 Service '{}' is in an intermediate state...", service);
                                return false;
                            }
                        }
                        return true;
                    });
        }

        log.info("✅ All requested cluster services are verified healthy in Consul!");
    }
}
