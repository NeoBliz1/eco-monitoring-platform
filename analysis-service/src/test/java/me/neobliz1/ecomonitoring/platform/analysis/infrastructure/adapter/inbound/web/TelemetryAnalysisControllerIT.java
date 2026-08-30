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

import com.github.benmanes.caffeine.cache.Cache;
import lombok.extern.slf4j.Slf4j;
import me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.support.IntegrationTestSupport;
import me.neobliz1.ecomonitoring.platform.model.exception.EcoPlatformErrorCode;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.WeatherPacket;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.WeatherMap;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.cache.CacheManager;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.ComposeContainer;
import org.testcontainers.junit.jupiter.Container;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

@Slf4j
@AutoConfigureMockMvc
public class TelemetryAnalysisControllerIT extends IntegrationTestSupport {

    private static final String MIN_LAT_PARAM = "min-lat";
    private static final String MAX_LAT_PARAM = "max-lat";
    private static final String MIN_LON_PARAM = "min-lon";
    private static final String MAX_LON_PARAM = "max-lon";
    private static final String TARGET_TIMESTAMP_PARAM = "targetTimestamp";
    private static final double VALID_MIN_LAT = 54.5;
    private static final double VALID_MAX_LAT = 55.5;
    private static final double VALID_MIN_LON = -61.5;
    private static final double VALID_MAX_LON = -60.5;
    private static final String WEATHER_MAPS_CACHE_NAME = "weatherMaps";

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private CacheManager cacheManager;

    @Container
    @SuppressWarnings("unused")
    public static final ComposeContainer ENVIRONMENT = getComposeContainer();

    private long currentBucketFloor;

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
        double testLat = 55.0;
        double testLon = -61.0;
        WeatherPacket packet1 = createFullSensorPacket(stationId, currentBucketFloor+1000, testLat, testLon);
        WeatherPacket packet2 = createFullSensorPacket("test-station-2", currentBucketFloor+2000,
                testLat+0.05, testLon+0.05);

        sendPacket(packet1);
        sendPacket(packet2);
        sendFlushPackage(stationId, currentBucketFloor, testLat, testLon);

        String gridCellKey = calculateGridCellKey(testLat, testLon);
        WeatherMap map = findWeatherMapByGridCellAnBucketFloor(gridCellKey, currentBucketFloor);
        assertGridCellExists(map, gridCellKey);
    }

    @Test
    public void shouldReturnWeatherMap_whenValidRequestWithinSameBucket() throws Exception {
        long targetTimestamp = currentBucketFloor+300000;

        mockMvc.perform(get(WEATHER_MAP_URI+LATEST_WEATHER_MAP_ENDPOINT)
                        .param(TARGET_TIMESTAMP_PARAM, String.valueOf(targetTimestamp))
                        .param(MIN_LAT_PARAM, String.valueOf(VALID_MIN_LAT))
                        .param(MAX_LAT_PARAM, String.valueOf(VALID_MAX_LAT))
                        .param(MIN_LON_PARAM, String.valueOf(VALID_MIN_LON))
                        .param(MAX_LON_PARAM, String.valueOf(VALID_MAX_LON))
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.timestampBucket").value(currentBucketFloor))
                .andExpect(jsonPath("$.intervalMinutes").value(10))
                .andExpect(jsonPath("$.gridCells").exists())
                .andExpect(jsonPath("$.gridCells.*.avgTemperature").value(22.5))
                .andExpect(jsonPath("$.gridCells.*.avgHumidity").value(55.0))
                .andExpect(jsonPath("$.gridCells.*.avgPressure").value(1013.25));
    }

    @Test
    public void shouldReturnWeatherMap_whenValidRequestWithCurrentTimestamp() throws Exception {
        mockMvc.perform(get(WEATHER_MAP_URI+LATEST_WEATHER_MAP_ENDPOINT)
                        .param(TARGET_TIMESTAMP_PARAM, String.valueOf(Instant.now().toEpochMilli()))
                        .param(MIN_LAT_PARAM, String.valueOf(VALID_MIN_LAT))
                        .param(MAX_LAT_PARAM, String.valueOf(VALID_MAX_LAT))
                        .param(MIN_LON_PARAM, String.valueOf(VALID_MIN_LON))
                        .param(MAX_LON_PARAM, String.valueOf(VALID_MAX_LON))
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.timestampBucket").value(currentBucketFloor))
                .andExpect(jsonPath("$.intervalMinutes").value(10))
                .andExpect(jsonPath("$.gridCells").exists())
                .andExpect(jsonPath("$.gridCells.*.avgTemperature").value(22.5))
                .andExpect(jsonPath("$.gridCells.*.avgHumidity").value(55.0))
                .andExpect(jsonPath("$.gridCells.*.avgPressure").value(1013.25));
    }

    @Test
    void shouldEvictCacheDataAutomatically_whenTenSecondsElapsed() throws Exception {
        long targetTimestamp = currentBucketFloor+300000;

        mockMvc.perform(get(WEATHER_MAP_URI+LATEST_WEATHER_MAP_ENDPOINT)
                        .param(TARGET_TIMESTAMP_PARAM, String.valueOf(targetTimestamp))
                        .param(MIN_LAT_PARAM, String.valueOf(VALID_MIN_LAT))
                        .param(MAX_LAT_PARAM, String.valueOf(VALID_MAX_LAT))
                        .param(MIN_LON_PARAM, String.valueOf(VALID_MIN_LON))
                        .param(MAX_LON_PARAM, String.valueOf(VALID_MAX_LON))
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk());

        @SuppressWarnings("unchecked")
        Cache<Object, Object> nativeCache =
                (Cache<Object, Object>) Objects.requireNonNull(cacheManager.getCache(WEATHER_MAPS_CACHE_NAME)).getNativeCache();
        Object dynamicSpringCacheKey = nativeCache.asMap().keySet().stream()
                .findFirst()
                .orElseThrow(() -> new AssertionError("Cache engine failed to create any entries!"));
        assertNotNull(nativeCache.asMap().get(dynamicSpringCacheKey), "Cache should be populated after first call");
        await().atMost(12, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    Object activeWrapper = nativeCache.asMap().get(dynamicSpringCacheKey);
                    assertNull(activeWrapper, "The cache record should have been evicted automatically after 10s!");
                });

        mockMvc.perform(get(WEATHER_MAP_URI+LATEST_WEATHER_MAP_ENDPOINT)
                        .param(TARGET_TIMESTAMP_PARAM, String.valueOf(targetTimestamp))
                        .param(MIN_LAT_PARAM, String.valueOf(VALID_MIN_LAT))
                        .param(MAX_LAT_PARAM, String.valueOf(VALID_MAX_LAT))
                        .param(MIN_LON_PARAM, String.valueOf(VALID_MIN_LON))
                        .param(MAX_LON_PARAM, String.valueOf(VALID_MAX_LON))
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk());
    }

    @Test
    public void shouldReturnNotFound_whenNoWeatherMapDataAvailable() throws Exception {
        long targetTimestamp = currentBucketFloor+86400000;
        String exMsg = "Weather map data not found for the requested time interval and coordinates.";

        mockMvc.perform(get(WEATHER_MAP_URI+LATEST_WEATHER_MAP_ENDPOINT)
                        .param(TARGET_TIMESTAMP_PARAM, String.valueOf(targetTimestamp))
                        .param(MIN_LAT_PARAM, String.valueOf(VALID_MIN_LAT))
                        .param(MAX_LAT_PARAM, String.valueOf(VALID_MAX_LAT))
                        .param(MIN_LON_PARAM, String.valueOf(VALID_MIN_LON))
                        .param(MAX_LON_PARAM, String.valueOf(VALID_MAX_LON))
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.errorCode").value(EcoPlatformErrorCode.WEATHER_MAP_DATA_NOT_FOUND.getCodeStr()))
                .andExpect(jsonPath("$.errorDescription").value(exMsg));
    }

    @Test
    public void shouldReturnBadRequest_whenLatitudeExceedsUpperBoundary() throws Exception {
        String exMsg = "Coordinates out of legal boundaries. Valid ranges: latitude -90 to 90, longitude -180 to 180.";

        mockMvc.perform(get(WEATHER_MAP_URI+LATEST_WEATHER_MAP_ENDPOINT)
                        .param(TARGET_TIMESTAMP_PARAM, String.valueOf(currentBucketFloor))
                        .param(MIN_LAT_PARAM, "89.0")
                        .param(MAX_LAT_PARAM, "91.0")
                        .param(MIN_LON_PARAM, "-61.0")
                        .param(MAX_LON_PARAM, "-60.0")
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errorCode").value(EcoPlatformErrorCode.INVALID_COORDINATES_BOUNDARIES.getCodeStr()))
                .andExpect(jsonPath("$.errorDescription").value(exMsg));
    }

    @Test
    public void shouldReturnBadRequest_whenLongitudeExceedsUpperBoundary() throws Exception {
        String exMsg = "Coordinates out of legal boundaries. Valid ranges: latitude -90 to 90, longitude -180 to 180.";

        mockMvc.perform(get(WEATHER_MAP_URI+LATEST_WEATHER_MAP_ENDPOINT)
                        .param(TARGET_TIMESTAMP_PARAM, String.valueOf(currentBucketFloor))
                        .param(MIN_LAT_PARAM, "55.0")
                        .param(MAX_LAT_PARAM, "56.0")
                        .param(MIN_LON_PARAM, "-179.0")
                        .param(MAX_LON_PARAM, "181.0")
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errorCode").value(EcoPlatformErrorCode.INVALID_COORDINATES_BOUNDARIES.getCodeStr()))
                .andExpect(jsonPath("$.errorDescription").value(exMsg));
    }

    @Test
    public void shouldReturnBadRequest_whenCoordinatesSquareExceedsMaximumDelta() throws Exception {
        String exMsg = "Requested bounding box area is too large. Maximum delta allowed is 5.0 degrees.";

        mockMvc.perform(get(WEATHER_MAP_URI+LATEST_WEATHER_MAP_ENDPOINT)
                        .param(TARGET_TIMESTAMP_PARAM, String.valueOf(currentBucketFloor))
                        .param(MIN_LAT_PARAM, "50.0")
                        .param(MAX_LAT_PARAM, "60.0")
                        .param(MIN_LON_PARAM, "-70.0")
                        .param(MAX_LON_PARAM, "-60.0")
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errorCode").value(EcoPlatformErrorCode.COORDINATES_SQUARE_TOO_LARGE.getCodeStr()))
                .andExpect(jsonPath("$.errorDescription").value(exMsg));
    }

    @Test
    public void shouldReturnBadRequest_whenTimestampIsNegative() throws Exception {
        String exMsg = "Invalid request parameters: getWeatherMapByTimeAndCoordinatesSquare.targetTimestamp: Timestamp cannot be negative";

        mockMvc.perform(get(WEATHER_MAP_URI+LATEST_WEATHER_MAP_ENDPOINT)
                        .param(TARGET_TIMESTAMP_PARAM, "-1000")
                        .param(MIN_LAT_PARAM, "55.0")
                        .param(MAX_LAT_PARAM, "56.0")
                        .param(MIN_LON_PARAM, "-61.0")
                        .param(MAX_LON_PARAM, "-60.0")
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errorCode").value(HttpStatus.BAD_REQUEST.toString()))
                .andExpect(jsonPath("$.errorDescription").value(exMsg));
    }

    @Test
    public void shouldReturnBadRequest_whenTimestampExceedsYear2100() throws Exception {
        String exMsg = "Invalid request parameters: getWeatherMapByTimeAndCoordinatesSquare.targetTimestamp: "
                +"Timestamp cannot be unreasonably far in the future (Max: Year 2100)";

        mockMvc.perform(get(WEATHER_MAP_URI+LATEST_WEATHER_MAP_ENDPOINT)
                        .param(TARGET_TIMESTAMP_PARAM, "4102444800001")
                        .param(MIN_LAT_PARAM, "55.0")
                        .param(MAX_LAT_PARAM, "56.0")
                        .param(MIN_LON_PARAM, "-61.0")
                        .param(MAX_LON_PARAM, "-60.0")
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errorCode").value(HttpStatus.BAD_REQUEST.toString()))
                .andExpect(jsonPath("$.errorDescription").value(exMsg));
    }

    @Test
    public void shouldReturnBadRequest_whenLatitudeBelowLowerBoundary() throws Exception {
        String exMsg = "Coordinates out of legal boundaries. Valid ranges: latitude -90 to 90, longitude -180 to 180.";

        mockMvc.perform(get(WEATHER_MAP_URI+LATEST_WEATHER_MAP_ENDPOINT)
                        .param(TARGET_TIMESTAMP_PARAM, String.valueOf(currentBucketFloor))
                        .param(MIN_LAT_PARAM, "-91.0")
                        .param(MAX_LAT_PARAM, "-89.0")
                        .param(MIN_LON_PARAM, "-61.0")
                        .param(MAX_LON_PARAM, "-60.0")
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errorCode").value(EcoPlatformErrorCode.INVALID_COORDINATES_BOUNDARIES.getCodeStr()))
                .andExpect(jsonPath("$.errorDescription").value(exMsg));
    }

    @Test
    public void shouldReturnBadRequest_whenLongitudeBelowLowerBoundary() throws Exception {
        String exMsg = "Coordinates out of legal boundaries. Valid ranges: latitude -90 to 90, longitude -180 to 180.";

        mockMvc.perform(get(WEATHER_MAP_URI+LATEST_WEATHER_MAP_ENDPOINT)
                        .param(TARGET_TIMESTAMP_PARAM, String.valueOf(currentBucketFloor))
                        .param(MIN_LAT_PARAM, "55.0")
                        .param(MAX_LAT_PARAM, "56.0")
                        .param(MIN_LON_PARAM, "-181.0")
                        .param(MAX_LON_PARAM, "-179.0")
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errorCode").value(EcoPlatformErrorCode.INVALID_COORDINATES_BOUNDARIES.getCodeStr()))
                .andExpect(jsonPath("$.errorDescription").value(exMsg));
    }
}
