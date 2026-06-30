package me.neobliz1.ecomonitoring.platform.ingestion.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.AmbientReading;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.Location;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.SensorReading;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.WeatherPacket;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;

import java.time.Instant;
import java.util.List;

public final class TestUtils {

    public static final String STATION_ID = "15";
    public static final String SYNC_SINGLE_URL = "/api/v1/telemetry/virtual";
    public static final String REACTIVE_MONO_URL = "/api/v1/telemetry/mono";

    private TestUtils() {
    }

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
                    assertEquals(1, violationsL.size());
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

        assertEquals(targetSnippet, matchedViolation, "Expected violation details missing from payload output! Got: "+violations);
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
}
