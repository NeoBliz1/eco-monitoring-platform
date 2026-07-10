package me.neobliz1.ecomonitoring.platform.common.util;

import lombok.experimental.UtilityClass;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.AmbientReading;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.Location;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.SensorReading;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.WeatherPacket;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Assertions;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;

import java.time.Instant;
import java.util.List;
import java.util.Map;

@UtilityClass
public class WeatherTestUtils {

    public static final String STATION_ID = "15";
    public static final String SYNC_SINGLE_URL = "/api/v1/telemetry/virtual";
    public static final String REACTIVE_MONO_URL = "/api/v1/telemetry/mono";

    public static void performValidPost(WebTestClient webTestClient, String uri, WeatherPacket.Builder builder) {
        byte[] rawProtoBytes = builder.build().toByteArray();

        webTestClient.post().uri(uri)
                .contentType(MediaType.APPLICATION_PROTOBUF)
                .bodyValue(rawProtoBytes)
                .exchange()
                .expectStatus().isAccepted();
    }

    @SuppressWarnings("unchecked")
    public static void performInvalidPost(WebTestClient webTestClient, String uri, WeatherPacket.Builder builder, String expectedViolationSnippet) {
        byte[] rawProtoBytes = builder.build().toByteArray();

        webTestClient.post().uri(uri)
                .contentType(MediaType.APPLICATION_PROTOBUF)
                .bodyValue(rawProtoBytes)
                .exchange()
                .expectStatus().isBadRequest()
                .expectBody()
                // Cast to List so we can isolate individual array elements cleanly
                .jsonPath("$.violations").value(violations -> {
                    List<String> violationsL = (List<String>) violations;
                    Assertions.assertEquals(1, violationsL.size());
                    assertErrorMessage(expectedViolationSnippet, violationsL);
                });
    }

    private static void assertErrorMessage(String expectedViolationSnippet, List<String> violations) {
        String targetSnippet = expectedViolationSnippet.toLowerCase();

        String matchedViolation = violations.stream()
                .map(String::toLowerCase)
                .filter(violation -> violation.contains(targetSnippet))
                .findFirst()
                .orElse(""); // If not found, empty string forces a clear JUnit diff report below

        Assertions.assertEquals(targetSnippet, matchedViolation, "Expected violation details missing from payload output! Got: "+violations);
    }

    public static WeatherPacket.Builder createValidBase() {
        return WeatherPacket.newBuilder()
                .setStationId(STATION_ID)
                .setTimestamp(Instant.now().toEpochMilli())
                .setLocation(Location.newBuilder()
                        .setLatitude(45.0)
                        .setLongitude(90.0)
                        .setAltitude(150.5))
                .addReadings(SensorReading.newBuilder()
                        .setAmbient(AmbientReading.newBuilder()
                                .setTemperatureC(22.5f)
                                .setHumidityPct(55.0f)
                                .setPressureHpa(1013.2f)
                                .setLeafWetnessPct(10.0f)));
    }

    /**
     * Your original custom consumer configuration builder method
     */
    public static Map<String, Object> getConsumerConf(String kafkaClient, String kafkaClientPwd, String kafkaServer) {
        String jaasFormat = String.format(
                "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";",
                kafkaClient, kafkaClientPwd
        );

        return Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer,
                ConsumerConfig.GROUP_ID_CONFIG, "integration-test-group-stable",
                CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT",
                SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-512",
                SaslConfigs.SASL_JAAS_CONFIG, jaasFormat,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed",
                ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, "false",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName()
        );
    }

    /**
     * Complementary producer configuration builder matching your SCRAM architecture parameters
     */
    public static Map<String, Object> getProducerConf(String kafkaClient, String kafkaClientPwd, String kafkaServer) {
        String jaasFormat = String.format(
                "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";",
                kafkaClient, kafkaClientPwd
        );

        return Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer,
                CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT",
                SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-512",
                SaslConfigs.SASL_JAAS_CONFIG, jaasFormat,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName()
        );
    }
}
