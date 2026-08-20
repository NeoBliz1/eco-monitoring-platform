package me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.inbound.web;

import static me.neobliz1.ecomonitoring.platform.common.api.uri.UriConstant.LATEST_WEATHER_MAP_ENDPOINT;
import static me.neobliz1.ecomonitoring.platform.common.api.uri.UriConstant.WEATHER_MAP_URI;
import static me.neobliz1.ecomonitoring.platform.test.common.util.WeatherTestUtils.waitForConsulServicesToBeHealthy;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import lombok.extern.slf4j.Slf4j;
import me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.support.IntegrationTestSupport;
import me.neobliz1.ecomonitoring.platform.model.exception.EcoPlatformErrorCode;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.WeatherPacket;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.WeatherMap;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.ComposeContainer;
import org.testcontainers.junit.jupiter.Container;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Slf4j
@AutoConfigureMockMvc
public class TelemetryAnalysisControllerIT extends IntegrationTestSupport {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private CacheManager cacheManager;

    @Container
    @SuppressWarnings("unused")
    public static final ComposeContainer ENVIRONMENT = getComposeContainer();

    private long currentBucketFloor;
    private final double testLat = 55.0;
    private final double testLon = -61.0;

    @BeforeAll
    static void beforeAll() {
        waitForConsulServicesToBeHealthy(List.of(
                "kafka",
                "schema-registry",
                "consul",
                "redis-cache"
        ));
    }

    @BeforeEach
    public void setup() throws Exception {
        currentBucketFloor = getCurrentBucketFloor();
        String stationId = "test-station-1";
        WeatherPacket packet1 = createFullSensorPacket(stationId, currentBucketFloor + 1000, testLat, testLon);
        WeatherPacket packet2 = createFullSensorPacket("test-station-2", currentBucketFloor+2000,
                testLat+0.05, testLon+0.05);
        
        sendPacket(packet1);
        sendPacket(packet2);
        sendFlushPackage(stationId, currentBucketFloor, testLat, testLon);
        
        // Wait for the WeatherMap to be processed and stored in Redis
        String gridCellKey = calculateGridCellKey(testLat, testLon);
        WeatherMap map = findWeatherMapByGridCellAnBucketFloor(gridCellKey, currentBucketFloor);
        assertGridCellExists(map, gridCellKey);
    }

    @Test
    public void shouldReturnWeatherMapWithValidData_whenValidRequest() throws Exception {
        long targetTimestamp = currentBucketFloor + 300000; // Within the same bucket
        String coordinatesSquare = getCoordinatesSquare();

        mockMvc.perform(get(WEATHER_MAP_URI+LATEST_WEATHER_MAP_ENDPOINT)
                .param("targetTimestamp", String.valueOf(targetTimestamp))
                .param("coordinates-square", coordinatesSquare)
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.timestamp_bucket").value(currentBucketFloor))
                .andExpect(jsonPath("$.interval_minutes").value(10))
                .andExpect(jsonPath("$.grid_cells").exists())
                .andExpect(jsonPath("$.grid_cells..avg_temperature").value(22.5))
                .andExpect(jsonPath("$.grid_cells..avg_humidity").value(55.0))
                .andExpect(jsonPath("$.grid_cells..avg_pressure").value(1013.25));

        mockMvc.perform(get(WEATHER_MAP_URI+LATEST_WEATHER_MAP_ENDPOINT)
                        .param("targetTimestamp", String.valueOf(Instant.now().toEpochMilli()))
                        .param("coordinates-square", coordinatesSquare)
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.timestamp_bucket").value(currentBucketFloor))
                .andExpect(jsonPath("$.interval_minutes").value(10))
                .andExpect(jsonPath("$.grid_cells").exists())
                .andExpect(jsonPath("$.grid_cells..avg_temperature").value(22.5))
                .andExpect(jsonPath("$.grid_cells..avg_humidity").value(55.0))
                .andExpect(jsonPath("$.grid_cells..avg_pressure").value(1013.25));
    }

    @Test
    public void shouldEvictCacheDataAutomatically_AfterTenSeconds() throws Exception {
        long targetTimestamp = currentBucketFloor+300000;
        String coordinatesSquareParam = "54.5,55.5,-61.5,-60.5";
        List<Double> expectedListStructure = List.of(54.5, 55.5, -61.5, -60.5);
        String expectedCacheKey = targetTimestamp+"#"+expectedListStructure;
        Cache weatherMapsCache = cacheManager.getCache("weatherMaps");
        assertNotNull(weatherMapsCache);

        mockMvc.perform(get(WEATHER_MAP_URI+LATEST_WEATHER_MAP_ENDPOINT)
                        .param("targetTimestamp", String.valueOf(targetTimestamp))
                        .param("coordinates-square", coordinatesSquareParam)
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk());

        assertNotNull(weatherMapsCache.get(expectedCacheKey), "Cache should be populated after first call");
        await().atMost(12, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    Cache.ValueWrapper activeWrapper = weatherMapsCache.get(expectedCacheKey);
                    assertNull(activeWrapper, "The cache record should have been evicted automatically after 10s!");
                });

        mockMvc.perform(get(WEATHER_MAP_URI+LATEST_WEATHER_MAP_ENDPOINT)
                        .param("targetTimestamp", String.valueOf(targetTimestamp))
                        .param("coordinates-square", coordinatesSquareParam)
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk());
    }

    @Test
    public void shouldReturnNotFound_whenNoWeatherMapDataAvailable() throws Exception {
        long targetTimestamp = currentBucketFloor + 86400000; // 24 hours later
        String coordinatesSquare = getCoordinatesSquare();
        String exMsg = "Weather map data not found for the requested time interval and coordinates.";

        mockMvc.perform(get(WEATHER_MAP_URI+LATEST_WEATHER_MAP_ENDPOINT)
                        .param("targetTimestamp", String.valueOf(targetTimestamp))
                        .param("coordinates-square", coordinatesSquare)
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.code").value(EcoPlatformErrorCode.WEATHER_MAP_DATA_NOT_FOUND.getCodeStr()))
                .andExpect(jsonPath("$.description").value(exMsg));
    }

    @Test
    public void shouldReturnBadRequest_whenCoordinatesSquareEmpty() throws Exception {
        String exMsg = "Invalid request parameters: getLatestFiveMinuteWeatherMapJson.arg1: "
                + "Coordinates square must contain exactly 4 parameters: minLat, maxLat, minLon, maxLon";

        mockMvc.perform(get(WEATHER_MAP_URI+LATEST_WEATHER_MAP_ENDPOINT)
                .param("targetTimestamp", String.valueOf(currentBucketFloor))
                .param("coordinates-square", "")
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value(HttpStatus.BAD_REQUEST.toString()))
                .andExpect(jsonPath("$.description").value(exMsg));
    }

    @Test
    public void shouldReturnBadRequest_whenCoordinatesSquareHasLessThan4Elements() throws Exception {
        String exMsg = "Invalid request parameters: getLatestFiveMinuteWeatherMapJson.arg1: "
                + "Coordinates square must contain exactly 4 parameters: minLat, maxLat, minLon, maxLon";

        mockMvc.perform(get(WEATHER_MAP_URI+LATEST_WEATHER_MAP_ENDPOINT)
                .param("targetTimestamp", String.valueOf(currentBucketFloor))
                .param("coordinates-square", "55.0,56.0")
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value(HttpStatus.BAD_REQUEST.toString()))
                .andExpect(jsonPath("$.description").value(exMsg));
    }

    @Test
    public void shouldReturnBadRequest_whenCoordinatesSquareHasMoreThan4Elements() throws Exception {
        String exMsg = "Invalid request parameters: getLatestFiveMinuteWeatherMapJson.arg1: "
                + "Coordinates square must contain exactly 4 parameters: minLat, maxLat, minLon, maxLon";

        mockMvc.perform(get(WEATHER_MAP_URI+LATEST_WEATHER_MAP_ENDPOINT)
                .param("targetTimestamp", String.valueOf(currentBucketFloor))
                .param("coordinates-square", "55.0,56.0,-61.0,-60.0,1.0")
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value(HttpStatus.BAD_REQUEST.toString()))
                .andExpect(jsonPath("$.description").value(exMsg));
    }

    @Test
    public void shouldReturnBadRequest_whenCoordinatesExceedLatitudeBoundaries() throws Exception {
        String exMsg = "GPS coordinates out of legal boundaries. Valid ranges: latitude -90 to 90, longitude -180 to 180.";

        mockMvc.perform(get(WEATHER_MAP_URI+LATEST_WEATHER_MAP_ENDPOINT)
                .param("targetTimestamp", String.valueOf(currentBucketFloor))
                .param("coordinates-square", "89.0,91.0,-61.0,-60.0")
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value(EcoPlatformErrorCode.INVALID_COORDINATES_BOUNDARIES.getCodeStr()))
                .andExpect(jsonPath("$.description").value(exMsg));
    }

    @Test
    public void shouldReturnBadRequest_whenCoordinatesExceedLongitudeBoundaries() throws Exception {
        String exMsg = "GPS coordinates out of legal boundaries. Valid ranges: latitude -90 to 90, longitude -180 to 180.";

        mockMvc.perform(get(WEATHER_MAP_URI+LATEST_WEATHER_MAP_ENDPOINT)
                .param("targetTimestamp", String.valueOf(currentBucketFloor))
                .param("coordinates-square", "55.0,56.0,-179.0,181.0")
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value(EcoPlatformErrorCode.INVALID_COORDINATES_BOUNDARIES.getCodeStr()))
                .andExpect(jsonPath("$.description").value(exMsg));
    }

    @Test
    public void shouldReturnBadRequest_whenCoordinatesSquareTooLarge() throws Exception {
        String exMsg = "Requested bounding box area is too large. Maximum delta allowed is 5.0 degrees.";

        mockMvc.perform(get(WEATHER_MAP_URI+LATEST_WEATHER_MAP_ENDPOINT)
                .param("targetTimestamp", String.valueOf(currentBucketFloor))
                .param("coordinates-square", "50.0,60.0,-70.0,-60.0")
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value(EcoPlatformErrorCode.COORDINATES_SQUARE_TOO_LARGE.getCodeStr()))
                .andExpect(jsonPath("$.description").value(exMsg));
    }

    @Test
    public void shouldReturnBadRequest_whenTimestampNegative() throws Exception {
        String exMsg = "Invalid request parameters: getLatestFiveMinuteWeatherMapJson.arg0: Timestamp cannot be negative";

        mockMvc.perform(get(WEATHER_MAP_URI+LATEST_WEATHER_MAP_ENDPOINT)
                .param("targetTimestamp", "-1000")
                .param("coordinates-square", "55.0,56.0,-61.0,-60.0")
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value(HttpStatus.BAD_REQUEST.toString()))
                .andExpect(jsonPath("$.description").value(exMsg));
    }

    @Test
    public void shouldReturnBadRequest_whenTimestampTooFarInFuture() throws Exception {
        String exMsg = "Invalid request parameters: getLatestFiveMinuteWeatherMapJson.arg0: "
                + "Timestamp cannot be unreasonably far in the future (Max: Year 2100)";

        mockMvc.perform(get(WEATHER_MAP_URI+LATEST_WEATHER_MAP_ENDPOINT)
                .param("targetTimestamp", "4102444800001")
                .param("coordinates-square", "55.0,56.0,-61.0,-60.0")
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value(HttpStatus.BAD_REQUEST.toString()))
                .andExpect(jsonPath("$.description").value(exMsg));
    }

    private @NonNull String getCoordinatesSquare() {
        return String.format("%.1f,%.1f,%.1f,%.1f", testLat-0.5, testLat+0.5, testLon-0.5, testLon+0.5);
    }
}
