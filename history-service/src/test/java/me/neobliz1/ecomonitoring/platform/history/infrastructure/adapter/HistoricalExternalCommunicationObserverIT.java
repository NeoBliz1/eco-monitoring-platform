package me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter;

import static me.neobliz1.ecomonitoring.platform.history.domain.model.constant.HistoricalCacheConstants.BUCKETS_REGION;
import static me.neobliz1.ecomonitoring.platform.history.domain.model.constant.HistoricalCacheConstants.QUERIES_REGION;
import static me.neobliz1.ecomonitoring.platform.test.common.util.WeatherTestUtils.INTERVAL_MINUTES;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.grpc.StatusRuntimeException;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.WeatherMap;
import me.neobliz1.ecomonitoring.platform.test.common.util.WeatherTestUtils;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import weather.history.SpatialBoxRequest;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

class HistoricalExternalCommunicationObserverIT extends IntegrationTestSupport {

    private static final double MIN_LAT = 40.0;
    private static final double MAX_LAT = 50.0;
    private static final double MIN_LON = 10.0;
    private static final double MAX_LON = 20.0;

    @Test
    void shouldReturnCorrectSpatialDataForBothTimeWindows_whenFourPacketsAreSeededViaKafka() throws Exception {
        long nowSeconds = Instant.now().getEpochSecond();
        long currentBucket = (nowSeconds/900)*900;
        long pastBucket = currentBucket-(25*3600);
        double minLat = 40.0;
        double maxLat = 50.0;
        double minLon = 10.0;
        double maxLon = 20.0;
        WeatherMap currentPacket1 = WeatherTestUtils.getCustomWeatherMap(currentBucket, "42.5#12.5", 25.0f);
        WeatherMap currentPacket2 = WeatherTestUtils.getCustomWeatherMap(currentBucket, "47.5#17.5", 26.5f);
        WeatherMap pastPacket1 = WeatherTestUtils.getCustomWeatherMap(pastBucket, "41.2#11.8", 12.0f);
        WeatherMap pastPacket2 = WeatherTestUtils.getCustomWeatherMap(pastBucket, "48.9#19.1", 14.5f);
        SpatialBoxRequest currentRequest = getSpatialBoxRequest(currentBucket, minLat, maxLat, minLon, maxLon);
        SpatialBoxRequest pastRequest = getSpatialBoxRequest(pastBucket, minLat, maxLat, minLon, maxLon);
        sendPacket(currentBucket, currentPacket1);
        sendPacket(currentBucket, currentPacket2);
        sendPacket(pastBucket, pastPacket1);
        sendPacket(pastBucket, pastPacket2);
        Awaitility.await()
                .atMost(Duration.ofMinutes(1))
                .pollInterval(Duration.ofMillis(500))
                .untilAsserted(() -> {
                    boolean currWeatherMapIsPresent = queryRepositoryAdapter.findByTimestampBucketAndIntervalMinutes(currentBucket, INTERVAL_MINUTES).isPresent();
                    boolean pastWeatherMapIsPresent = queryRepositoryAdapter.findByTimestampBucketAndIntervalMinutes(pastBucket, INTERVAL_MINUTES).isPresent();
                    assertTrue(currWeatherMapIsPresent);
                    assertTrue(pastWeatherMapIsPresent);
                });

        WeatherMap currentResponse = historyRemoteClientStub.findFilteredGridDataBySpatialBox(currentRequest);
        WeatherMap pastResponse = historyRemoteClientStub.findFilteredGridDataBySpatialBox(pastRequest);

        assertNotNull(currentResponse);
        assertEquals(currentBucket, currentResponse.getTimestampBucket());
        assertEquals(2, currentResponse.getGridCellsCount());
        assertTrue(currentResponse.containsGridCells("42.5#12.5"));
        assertTrue(currentResponse.containsGridCells("47.5#17.5"));
        assertNotNull(pastResponse);
        assertEquals(pastBucket, pastResponse.getTimestampBucket());
        assertEquals(2, pastResponse.getGridCellsCount());
        assertTrue(pastResponse.containsGridCells("41.2#11.8"));
        assertTrue(pastResponse.containsGridCells("48.9#19.1"));
    }

    @Test
    void shouldFallbackToClosestPastBucket_whenCurrentBucketHasNoGridCellsForRequestedSpatialBox() throws Exception {
        long nowSeconds = Instant.now().getEpochSecond();
        long currentBucket = (nowSeconds/900)*900;
        long pastBucket = currentBucket-7200;
        long futureBucket = currentBucket+7200;
        String targetedGeohash = "42.5#12.5";
        WeatherMap pastPacket = WeatherTestUtils.getCustomWeatherMap(pastBucket, targetedGeohash, 19.5f);
        WeatherMap futurePacket = WeatherTestUtils.getCustomWeatherMap(futureBucket, targetedGeohash, 19.5f);
        sendPacket(pastBucket, pastPacket);
        sendPacket(futureBucket, futurePacket);
        Awaitility.await()
                .atMost(Duration.ofMinutes(1))
                .pollInterval(Duration.ofMillis(500))
                .untilAsserted(() -> {
                    boolean pastWeatherMapIsPresent = queryRepositoryAdapter.findByTimestampBucketAndIntervalMinutes(pastBucket, INTERVAL_MINUTES).isPresent();
                    boolean futureWeatherMapIsPresent = queryRepositoryAdapter.findByTimestampBucketAndIntervalMinutes(futureBucket, INTERVAL_MINUTES).isPresent();
                    assertTrue(pastWeatherMapIsPresent);
                    assertTrue(futureWeatherMapIsPresent);
                });
        Objects.requireNonNull(springL1CacheManager.getCache(BUCKETS_REGION)).clear();
        Objects.requireNonNull(springL1CacheManager.getCache(QUERIES_REGION)).clear();
        SpatialBoxRequest fallbackRequest = getSpatialBoxRequest(currentBucket, MIN_LAT, MAX_LAT, MIN_LON, MAX_LON);

        WeatherMap response = historyRemoteClientStub.findFilteredGridDataBySpatialBox(fallbackRequest);

        assertNotNull(response);
        assertEquals(currentBucket, response.getTimestampBucket());
        assertEquals(INTERVAL_MINUTES, response.getIntervalMinutes());
        assertEquals(1, response.getGridCellsCount());
        assertTrue(response.containsGridCells(targetedGeohash));
    }

    @Test
    void shouldThrowStatusRuntimeException_whenAllHistoricalBucketsAreCompletelyExhausted() {
        long nowSeconds = Instant.now().getEpochSecond();
        long emptyCurrentBucket = (nowSeconds/900)*900;
        Objects.requireNonNull(springL1CacheManager.getCache(BUCKETS_REGION)).clear();
        Objects.requireNonNull(springL1CacheManager.getCache(QUERIES_REGION)).clear();
        SpatialBoxRequest exhaustiveRequest = getSpatialBoxRequest(emptyCurrentBucket, MIN_LAT, MAX_LAT, MIN_LON, MAX_LON);

        StatusRuntimeException exception = assertThrows(
                StatusRuntimeException.class,
                () -> historyRemoteClientStub.findFilteredGridDataBySpatialBox(exhaustiveRequest)
        );

        assertEquals(io.grpc.Status.Code.NOT_FOUND, exception.getStatus().getCode());
        assertNotNull(exception.getStatus().getDescription());
        assertTrue(exception.getStatus().getDescription().contains("No grid metrics found within the requested time or any past historical buckets."));
    }
}
