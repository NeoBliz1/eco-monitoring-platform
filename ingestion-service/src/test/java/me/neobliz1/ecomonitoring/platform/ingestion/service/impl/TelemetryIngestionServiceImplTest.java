package me.neobliz1.ecomonitoring.platform.ingestion.service.impl;

import static me.neobliz1.ecomonitoring.platform.ingestion.util.TestUtils.REACTIVE_MONO_URL;
import static me.neobliz1.ecomonitoring.platform.ingestion.util.TestUtils.SYNC_SINGLE_URL;
import static me.neobliz1.ecomonitoring.platform.ingestion.util.TestUtils.createValidBase;
import static me.neobliz1.ecomonitoring.platform.ingestion.util.TestUtils.getConsumerConf;
import static me.neobliz1.ecomonitoring.platform.ingestion.util.TestUtils.performValidPost;
import static org.junit.jupiter.api.Assertions.assertTrue;

import me.neobliz1.ecomonitoring.platform.common.serialization.WeatherPacketDeserializer;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.WeatherPacket;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webtestclient.autoconfigure.AutoConfigureWebTestClient;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@AutoConfigureWebTestClient
@TestPropertySource(locations = "classpath:.env.test")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class TelemetryIngestionServiceImplTest {

    private static final String TOPIC_NAME = "environment.weather.telemetry.live";
    private static final int PARTITION_COUNT = 6;
    @Autowired
    private WebTestClient webTestClient;
    private Consumer<String, WeatherPacket> consumer;
    @Value("${KAFKA_BOOTSTRAP_SERVERS}")
    private String kafkaServer;
    @Value("${KAFKA_CLIENT}")
    private String kafkaClient;
    @Value("${KAFKA_CLIENT_PASSWORD}")
    private String kafkaClientPwd;

    @BeforeEach
    public void setUp() {
        Map<String, Object> conf = getConsumerConf(kafkaClient, kafkaClientPwd, kafkaServer);

        consumer = new KafkaConsumer<>(conf, new StringDeserializer(), new WeatherPacketDeserializer());

        List<TopicPartition> partitions = IntStream.range(0, PARTITION_COUNT)
                .mapToObj(p -> new TopicPartition(TOPIC_NAME, p))
                .collect(Collectors.toList());

        consumer.assign(partitions);

        System.out.println("⏳ Warm-up connection layer handshake verification...");
        // Triggers initial network loop sync so that the cluster registers this static group profile
        consumer.poll(Duration.ofMillis(2000));
        System.out.println("✅ READY: Consumer group cached.");
    }

    @AfterEach
    public void tearDown() {
        if(consumer!=null) {
            consumer.close();
        }
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
        List<TopicPartition> partitions = IntStream.range(0, PARTITION_COUNT)
                .mapToObj(p -> new TopicPartition(TOPIC_NAME, p))
                .collect(Collectors.toList());

        consumer.seekToBeginning(partitions);

        List<WeatherPacket> receivedPackets = new ArrayList<>();

        Awaitility.await()
                .atMost(Duration.ofSeconds(12))
                .pollInterval(Duration.ofMillis(300))
                .untilAsserted(() -> {
                    for(ConsumerRecord<String, WeatherPacket> record : consumer.poll(Duration.ofMillis(500))) {
                        if(record.value()!=null) {
                            receivedPackets.add(record.value());
                        }
                    }

                    boolean foundMatch = receivedPackets.stream()
                            .anyMatch(packet -> targetStationId.equals(packet.getStationId()));

                    assertTrue(foundMatch, "Expected WeatherPacket was not received by the Kafka consumer group yet");
                });
    }
}
