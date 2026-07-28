package me.neobliz1.ecomonitoring.platform.ingestion.infrastructure.adapter.inbound.web;

import static me.neobliz1.ecomonitoring.platform.common.constant.PlatformConstants.SCHEMA_REGISTRY_URL;
import static me.neobliz1.ecomonitoring.platform.test.common.util.WeatherTestUtils.REACTIVE_MONO_URL;
import static me.neobliz1.ecomonitoring.platform.test.common.util.WeatherTestUtils.SYNC_SINGLE_URL;
import static me.neobliz1.ecomonitoring.platform.test.common.util.WeatherTestUtils.createValidBase;
import static me.neobliz1.ecomonitoring.platform.test.common.util.WeatherTestUtils.loadEnvironmentMap;
import static me.neobliz1.ecomonitoring.platform.test.common.util.WeatherTestUtils.performValidPost;
import static me.neobliz1.ecomonitoring.platform.test.common.util.WeatherTestUtils.waitForConsulServicesToBeHealthy;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.WeatherPacket;
import me.neobliz1.ecomonitoring.platform.test.common.listener.TestKafkaListener;
import me.neobliz1.ecomonitoring.platform.test.common.util.WeatherTestUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webtestclient.autoconfigure.AutoConfigureWebTestClient;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.testcontainers.containers.ComposeContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.File;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Testcontainers
@ActiveProfiles("dev")
@AutoConfigureWebTestClient
@TestPropertySource(locations = "classpath:.env.test")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class TelemetryInvocationControllerIT {

    @Container
    @SuppressWarnings("unused")
    public static final ComposeContainer ENVIRONMENT = new ComposeContainer(new File("../docker/docker-compose.yaml"))
            .withEnv(loadEnvironmentMap())
            .withRemoveVolumes(true)
            .withTailChildContainers(true);

    @DynamicPropertySource
    static void registerDynamicProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.topic.weather-live", () -> "environment.weather.telemetry.live");
        registry.add("spring.kafka.streams.properties.schema.registry.url", () -> "http://localhost:8085");
    }

    @Autowired
    private WebTestClient webTestClient;
    private Consumer<String, WeatherPacket> consumer;
    private TestKafkaListener<WeatherPacket> kafkaListener;

    @Value("${KAFKA_BOOTSTRAP_SERVERS}")
    private String kafkaServer;
    @Value("${KAFKA_CLIENT}")
    private String kafkaClient;
    @Value("${KAFKA_CLIENT_PASSWORD}")
    private String kafkaClientPwd;
    @Value("${spring.kafka.topic.weather-live}")
    private String kafkaIngestionLiveTopic;
    @Value("${spring.kafka.streams.properties.schema.registry.url}")
    private String schemaRegistryUrl;


    @BeforeAll
    static void beforeAll() {
        waitForConsulServicesToBeHealthy();
    }

    @BeforeEach
    public void setUp() {
        Map<String, Object> conf = WeatherTestUtils.getConsumerConf(kafkaClient, kafkaClientPwd, kafkaServer, schemaRegistryUrl);
        Map<String, Object> rawSrConfig = new HashMap<>();
        rawSrConfig.put(SCHEMA_REGISTRY_URL, schemaRegistryUrl);
        rawSrConfig.put("specific.protobuf.value.type", WeatherPacket.class);
        KafkaProtobufDeserializer<WeatherPacket> rawDeserializer = new KafkaProtobufDeserializer<>();
        rawDeserializer.configure(rawSrConfig, false);
        consumer = new KafkaConsumer<>(conf, new StringDeserializer(), rawDeserializer);
        consumer.subscribe(Collections.singletonList(kafkaIngestionLiveTopic),
                new ConsumerRebalanceListener() {
                    @Override
                    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    }

                    @Override
                    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                        consumer.seekToBeginning(partitions);
                    }
                });
        kafkaListener = new TestKafkaListener<>(consumer);
    }

    @AfterEach
    public void tearDown() {
        kafkaListener.close();
    }

    @Test
    void shouldSuccessPollPacketFromKafka_whenSendItByBlockingWayViaVector() {
        String sId = "14";

        performValidPost(webTestClient, SYNC_SINGLE_URL, createValidBase().setStationId(sId));

        assertDelivery(sId);
    }

    @Test
    void shouldSuccessPollPacketFromKafka_whenSendItByReactiveWayViaVector() {
        String sId = "18";

        performValidPost(webTestClient, REACTIVE_MONO_URL, createValidBase().setStationId(sId));

        assertDelivery(sId);
    }

    private void assertDelivery(String targetStationId) {
        Awaitility.await()
                .atMost(Duration.ofMinutes(1))
                .untilAsserted(() -> {
                    List<WeatherPacket> receivedPackets = kafkaListener.getReceivedPackets();
                    boolean foundMatch = receivedPackets.stream()
                            .anyMatch(packet -> targetStationId.equals(packet.getStationId()));
                    assertTrue(foundMatch, "Expected WeatherPacket was not received by the Kafka consumer group yet");
                });
    }
}
