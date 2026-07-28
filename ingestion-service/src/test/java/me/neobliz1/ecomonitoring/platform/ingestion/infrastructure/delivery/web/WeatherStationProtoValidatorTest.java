package me.neobliz1.ecomonitoring.platform.ingestion.infrastructure.delivery.web;

import static me.neobliz1.ecomonitoring.platform.test.common.util.WeatherTestUtils.REACTIVE_MONO_URL;
import static me.neobliz1.ecomonitoring.platform.test.common.util.WeatherTestUtils.SYNC_SINGLE_URL;
import static me.neobliz1.ecomonitoring.platform.test.common.util.WeatherTestUtils.createValidBase;
import static me.neobliz1.ecomonitoring.platform.test.common.util.WeatherTestUtils.performInvalidPost;
import static me.neobliz1.ecomonitoring.platform.test.common.util.WeatherTestUtils.performValidPost;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import io.github.neobliz1.validproto.config.HttpValidateProtoAutoConfiguration;
import me.neobliz1.ecomonitoring.platform.ingestion.domain.service.TelemetryIngestionService;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.AirQualityReading;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.AmbientReading;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.SensorReading;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.WeatherPacket;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.WindReading;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.webflux.test.autoconfigure.WebFluxTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.core.annotation.Order;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Mono;

@Disabled
@Import(HttpValidateProtoAutoConfiguration.class)
@TestPropertySource(locations = "classpath:.env.test")
@WebFluxTest(controllers = TelemetryInvocationController.class)
public class WeatherStationProtoValidatorTest {

    @MockitoBean
    @SuppressWarnings("unused")
    private TelemetryIngestionService telemetryService;

    @Autowired
    private WebTestClient webTestClient;

    @BeforeEach
    public void setUp() {
        when(this.telemetryService.processTelemetryPacket(any())).thenReturn(Mono.just(true));
        when(this.telemetryService.processTelemetryPacketVirtual(any())).thenReturn(true);
    }

    @ParameterizedTest
    @ValueSource(strings = { SYNC_SINGLE_URL, REACTIVE_MONO_URL })
    void shouldReturn200_whenPayloadIsValid(String url) {
        performValidPost(webTestClient, url, createValidBase());
    }

    @ParameterizedTest
    @ValueSource(strings = { SYNC_SINGLE_URL, REACTIVE_MONO_URL })
    void shouldReturn200_whenStationIdIsAtLowerBoundary(String url) {
        performValidPost(webTestClient, url, createValidBase().setStationId("1"));
    }

    @ParameterizedTest
    @ValueSource(strings = { SYNC_SINGLE_URL, REACTIVE_MONO_URL })
    void shouldReturn200_whenStationIdIsAtUpperBoundary(String url) {
        performValidPost(webTestClient, url, createValidBase().setStationId("30"));
    }

    @ParameterizedTest
    @ValueSource(strings = { SYNC_SINGLE_URL, REACTIVE_MONO_URL })
    void shouldReturn400_whenLocationIsMissing(String url) {
        performInvalidPost(webTestClient, url, createValidBase().clearLocation(), "location: value is required");
    }

    @ParameterizedTest
    @ValueSource(strings = { SYNC_SINGLE_URL, REACTIVE_MONO_URL })
    void shouldReturn400_whenLatitudeExceedsMaxBounds(String url) {
        WeatherPacket.Builder base = createValidBase();
        base.getLocationBuilder().setLatitude(90.1);
        performInvalidPost(webTestClient, url, base, "location: must be greater than or equal to -90 and less than or equal to 90");
    }

    @ParameterizedTest
    @ValueSource(strings = { SYNC_SINGLE_URL, REACTIVE_MONO_URL })
    void shouldReturn400_whenLongitudeExceedsMinBounds(String url) {
        WeatherPacket.Builder base = createValidBase();
        base.getLocationBuilder().setLongitude(-180.1);

        performInvalidPost(webTestClient, url, base, "location: must be greater than or equal to -180 and less than or equal to 180");
    }

    @ParameterizedTest
    @ValueSource(strings = { SYNC_SINGLE_URL, REACTIVE_MONO_URL })
    void shouldReturn400_whenReadingsListIsEmpty(String url) {
        performInvalidPost(webTestClient, url, createValidBase().clearReadings(), "readings: must contain at least 1 item(s)");
    }

    @ParameterizedTest
    @ValueSource(strings = { SYNC_SINGLE_URL, REACTIVE_MONO_URL })
    void shouldReturn400_whenSensorDataOneofIsMissing(String url) {
        WeatherPacket.Builder base = createValidBase();
        base.clearReadings().addReadings(SensorReading.newBuilder());

        performInvalidPost(webTestClient, url, base, "readings: exactly one field is required in oneof");
    }

    @ParameterizedTest
    @ValueSource(strings = { SYNC_SINGLE_URL, REACTIVE_MONO_URL })
    void shouldReturn400_whenWindSpeedIsNegative(String url) {
        WeatherPacket.Builder base = createValidBase();
        base.clearReadings().addReadings(SensorReading.newBuilder()
                .setWind(WindReading.newBuilder().setSpeedMps(-0.1f)));

        performInvalidPost(webTestClient, url, base, "readings: must be greater than or equal to 0");
    }

    @ParameterizedTest
    @ValueSource(strings = { SYNC_SINGLE_URL, REACTIVE_MONO_URL })
    void shouldReturn400_whenAmbientHumidityExceedsPercentage(String url) {
        WeatherPacket.Builder base = createValidBase();
        base.clearReadings().addReadings(SensorReading.newBuilder()
                .setAmbient(AmbientReading.newBuilder().setHumidityPct(100.1f)));

        performInvalidPost(webTestClient, url, base, "readings: must be greater than or equal to 0 and less than or equal to 100");
    }

    @ParameterizedTest
    @ValueSource(strings = { SYNC_SINGLE_URL, REACTIVE_MONO_URL })
    void shouldReturn400_whenAirQualityPm25IsNegative(String url) {
        WeatherPacket.Builder base = createValidBase();
        base.clearReadings().addReadings(SensorReading.newBuilder()
                .setAirQuality(AirQualityReading.newBuilder().setPm25(-1.0f)));

        performInvalidPost(webTestClient, url, base, "readings: must be greater than or equal to 0");
    }

    @TestConfiguration
    static class TestConfig {
        @Bean
        @Order(-2)
        public ReactiveValidationWebExceptionHandler reactiveValidationWebExceptionHandler() {
            return new ReactiveValidationWebExceptionHandler();
        }
    }
}
