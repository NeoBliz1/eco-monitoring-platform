package me.neobliz1.ecomonitoring.platform.test.common.util;

import static java.util.Map.entry;
import static me.neobliz1.ecomonitoring.platform.common.constant.PlatformConstants.SCHEMA_REGISTRY_URL;
import static org.apache.kafka.common.security.scram.internals.ScramMechanism.SCRAM_SHA_512;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.AmbientReading;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.Location;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.SensorReading;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.WeatherPacket;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.internals.AutoOffsetResetStrategy.StrategyType;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Slf4j
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
                .orElse("");

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

    public static Map<String, Object> getConsumerConf(String kafkaClient, String kafkaClientPwd, String kafkaServer, String schemaRegistryUrl) {
        String jaasFormat = String.format(
                "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";",
                kafkaClient, kafkaClientPwd
        );

        return Map.ofEntries(
                entry(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer),
                entry(ConsumerConfig.GROUP_ID_CONFIG, "integration-test-group-stable"),
                entry(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name),
                entry(SaslConfigs.SASL_MECHANISM, SCRAM_SHA_512.mechanismName()),
                entry(SaslConfigs.SASL_JAAS_CONFIG, jaasFormat),
                entry(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, StrategyType.EARLIEST.toString()),
                entry(ConsumerConfig.ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_COMMITTED.toString()),
                entry(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, "false"),
                entry(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()),
                entry(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName()),
                entry(SCHEMA_REGISTRY_URL, schemaRegistryUrl)
        );
    }

    public static Map<String, Object> getTestKafkaAdminConf(String kafkaAdmin, String kafkaAdminPwd, String kafkaServer) {
        String jaasFormat = String.format(
                "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";",
                kafkaAdmin, kafkaAdminPwd
        );

        return Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer,
                CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name,
                SaslConfigs.SASL_MECHANISM, SCRAM_SHA_512.mechanismName(),
                SaslConfigs.SASL_JAAS_CONFIG, jaasFormat
        );
    }

    public static Map<String, Object> getProducerConf(String kafkaClient, String kafkaClientPwd, String kafkaServer, String schemaRegistryUrl) {
        String jaasFormat = String.format(
                "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";",
                kafkaClient, kafkaClientPwd
        );
        return Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer,
                CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name,
                SaslConfigs.SASL_MECHANISM, SCRAM_SHA_512.mechanismName(),
                SaslConfigs.SASL_JAAS_CONFIG, jaasFormat,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer",
                SCHEMA_REGISTRY_URL, schemaRegistryUrl
        );
    }

    public static Map<String, String> loadEnvironmentMap() {
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

    public static void waitForConsulServicesToBeHealthy() {
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
                            String body = response.body();
                            if(Objects.isNull(body)) {
                                log.info("▶️ Service '{}' status is unknow, response body is null", service);
                                return false;
                            }
                            String trimBody = body.trim();
                            if("[]".equals(trimBody) || trimBody.isEmpty()) {
                                log.info("▶️ Service '{}' is not registered yet...", service);
                                return false;
                            }
                            if(trimBody.contains("\"Status\":\"critical\"") || trimBody.contains("\"Status\":\"warning\"")) {
                                log.info("⚠️ Service '{}' has checks that are failing or warming up...", service);
                                return false;
                            }
                            if(!trimBody.contains("\"Status\":\"passing\"")) {
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
