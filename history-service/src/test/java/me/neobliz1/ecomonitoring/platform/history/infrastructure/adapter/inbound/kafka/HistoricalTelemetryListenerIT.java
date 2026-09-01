package me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter.inbound.kafka;

import static me.neobliz1.ecomonitoring.platform.test.common.util.WeatherTestUtils.GEOHASH_ALPHA;
import static me.neobliz1.ecomonitoring.platform.test.common.util.WeatherTestUtils.INTERVAL_MINUTES;
import static me.neobliz1.ecomonitoring.platform.test.common.util.WeatherTestUtils.loadEnvironmentMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import me.neobliz1.ecomonitoring.platform.history.domain.model.entity.WeatherGridCellMetric;
import me.neobliz1.ecomonitoring.platform.history.domain.model.entity.WeatherMapBucket;
import me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter.outbound.persistence.postgres.HistoricalWeatherMapJpaRepository;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.GridCellLayers;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.WeatherMap;
import me.neobliz1.ecomonitoring.platform.test.common.util.WeatherTestUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.testcontainers.containers.ComposeContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import weather.history.SpatialBoxRequest;

import java.io.File;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Testcontainers
public class HistoricalTelemetryListenerIT extends IntegrationTestSupport {

    @Container
    @SuppressWarnings("unused")
    public static final ComposeContainer ENVIRONMENT = new ComposeContainer(new File("../docker/history-test-docker-compose.yaml"))
            .withEnv(loadEnvironmentMap())
            .withExposedService(PG_DB, PG_DB_PORT)
            .withRemoveVolumes(true)
            .withTailChildContainers(true);

    @MockitoSpyBean
    private HistoricalWeatherMapJpaRepository queryRepositoryAdapter;

    @BeforeAll
    static void beforeAll() {
        IntegrationTestSupport.beforeAll();
        runLiquibaseMigrationsOnTestComposeCluster();
    }

    @Test
    void shouldPersistRecordAndAcknowledgeKafkaOffset_whenPayloadIsProcessedSuccessfully() throws Exception {
        WeatherMap weatherMap = WeatherTestUtils.getWeatherMap();
        long timestampBucket = weatherMap.getTimestampBucket();

        sendPacket(timestampBucket, weatherMap);
        Awaitility.await()
                .atMost(Duration.ofMinutes(1))
                .pollInterval(Duration.ofMillis(500))
                .untilAsserted(() -> assertTrue(
                        queryRepositoryAdapter.findByTimestampBucketAndIntervalMinutes(timestampBucket, INTERVAL_MINUTES)
                                .isPresent()
                ));
        WeatherMapBucket weatherMapBucket = getWeatherMapBucket(timestampBucket);

        assertEquals(timestampBucket, weatherMapBucket.getTimestampBucket());
        assertEquals(INTERVAL_MINUTES, weatherMapBucket.getIntervalMinutes());
        assertEquals(GEOHASH_ALPHA, weatherMapBucket.getGridCells().getFirst().getGeohash());
    }

    @Test
    void shouldReturnExistingBucketWithFirstGeneratedId_whenDuplicatePayloadIsProcessedSerially() throws Exception {
        WeatherMap weatherMap = WeatherTestUtils.getWeatherMap();
        long targetTimestampBucket = weatherMap.getTimestampBucket();
        ArgumentCaptor<UUID> uuidCaptor = ArgumentCaptor.forClass(UUID.class);

        sendPacket(targetTimestampBucket, weatherMap);
        Awaitility.await()
                .atMost(Duration.ofSeconds(15))
                .pollInterval(Duration.ofMillis(300))
                .untilAsserted(() -> assertTrue(
                        queryRepositoryAdapter.findByTimestampBucketAndIntervalMinutes(targetTimestampBucket, INTERVAL_MINUTES).isPresent()
                ));
        WeatherMapBucket firstBucket = getWeatherMapBucket(targetTimestampBucket);
        UUID expectedBucketId = firstBucket.getId();
        Mockito.clearInvocations(queryRepositoryAdapter);
        sendPacket(targetTimestampBucket, weatherMap);
        Awaitility.await()
                .atMost(Duration.ofSeconds(15))
                .pollInterval(Duration.ofMillis(300))
                .untilAsserted(() -> Mockito.verify(queryRepositoryAdapter, Mockito.atLeastOnce())
                        .upsertBucket(uuidCaptor.capture(), Mockito.eq(targetTimestampBucket), Mockito.eq(INTERVAL_MINUTES)));
        WeatherMapBucket secondBucket = getWeatherMapBucket(targetTimestampBucket);
        long bucketRowCount = queryJpaRepositoryAdapter.count();
        UUID capturedSecondUuid = uuidCaptor.getValue();

        assertEquals(1L, bucketRowCount);
        assertNotEquals(expectedBucketId, capturedSecondUuid);
        assertEquals(expectedBucketId, secondBucket.getId());
        assertEquals(targetTimestampBucket, secondBucket.getTimestampBucket());
    }

    @Test
    void shouldRollbackTransactionAndKeepOffsetUncommitted_whenDatabaseCheckConstraintIsViolated() throws Exception {
        long timestampBucket = 1819234000L;
        WeatherMap poisonousMap = WeatherMap.newBuilder()
                .setTimestampBucket(timestampBucket)
                .setIntervalMinutes(INTERVAL_MINUTES)
                .putGridCells(GEOHASH_ALPHA, GridCellLayers.newBuilder()
                        .setReadingCount(1)
                        .setAvgHumidity(150.0)
                        .build())
                .build();

        sendPacket(timestampBucket, poisonousMap);

        Awaitility.await()
                .atMost(Duration.ofSeconds(10))
                .during(Duration.ofSeconds(3))
                .until(() -> queryRepositoryAdapter
                        .findByTimestampBucketAndIntervalMinutes(timestampBucket, INTERVAL_MINUTES)
                        .isEmpty());
        List<WeatherMapBucket> bucketList = queryJpaRepositoryAdapter.findAll();
        assertTrue(bucketList.isEmpty());
    }

    @Test
    void shouldRejectPayloadAndHaltProgression_whenGridCellContainsZeroActualMetricGroupReadings() throws Exception {
        long timestampBucket = 1919234000L;
        WeatherMap emptyPayload = WeatherMap.newBuilder()
                .setTimestampBucket(timestampBucket)
                .setIntervalMinutes(INTERVAL_MINUTES)
                .putGridCells(GEOHASH_ALPHA, GridCellLayers.newBuilder()
                        .setReadingCount(5)
                        .build())
                .build();

        sendPacket(timestampBucket, emptyPayload);

        Awaitility.await()
                .atMost(Duration.ofMinutes(1))
                .during(Duration.ofSeconds(3))
                .until(() -> queryRepositoryAdapter
                        .findByTimestampBucketAndIntervalMinutes(timestampBucket, INTERVAL_MINUTES)
                        .isEmpty());
        List<WeatherMapBucket> bucketList = queryJpaRepositoryAdapter.findAll();
        assertTrue(bucketList.isEmpty());
    }

    @Test
    void shouldAccumulateDistinctGeohashMetricsIntoSingleBucket_whenTwoDistinctPayloadsArriveSerially() throws Exception {
        long timestampBucket = 2119234000L;
        WeatherMap alphaPayload = WeatherTestUtils.getCustomWeatherMap(timestampBucket, GEOHASH_ALPHA, 22.5f);
        String geohash2 = "w21z7";
        WeatherMap betaPayload = WeatherTestUtils.getCustomWeatherMap(timestampBucket, geohash2, 18.2f);

        sendPacket(timestampBucket, alphaPayload);
        sendPacket(timestampBucket, betaPayload);
        Awaitility.await()
                .atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(300))
                .untilAsserted(() -> {
                    WeatherMapBucket weatherMapBucket = getWeatherMapBucket(timestampBucket);
                    assertEquals(2, weatherMapBucket.getGridCells().size());
                });
        WeatherMapBucket targetBucket = getWeatherMapBucket(timestampBucket);
        long bucketRowCount = queryJpaRepositoryAdapter.count();
        List<String> persistedGeohashes = targetBucket.getGridCells().stream()
                .map(WeatherGridCellMetric::getGeohash)
                .toList();

        assertEquals(1L, bucketRowCount);
        assertTrue(persistedGeohashes.contains(GEOHASH_ALPHA));
        assertTrue(persistedGeohashes.contains(geohash2));
    }

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
        SpatialBoxRequest currentRequest = SpatialBoxRequest.newBuilder()
                .setTimestampBucket(currentBucket)
                .setTimeIntervalInMinutes(INTERVAL_MINUTES)
                .setMinLat(minLat)
                .setMaxLat(maxLat)
                .setMinLon(minLon)
                .setMaxLon(maxLon)
                .build();
        SpatialBoxRequest pastRequest = SpatialBoxRequest.newBuilder()
                .setTimestampBucket(pastBucket)
                .setTimeIntervalInMinutes(INTERVAL_MINUTES)
                .setMinLat(minLat)
                .setMaxLat(maxLat)
                .setMinLon(minLon)
                .setMaxLon(maxLon)
                .build();

        sendPacket(currentBucket, currentPacket1);
        sendPacket(currentBucket, currentPacket2);
        sendPacket(pastBucket, pastPacket1);
        sendPacket(pastBucket, pastPacket2);
        Awaitility.await()
                .atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(500))
                .untilAsserted(() -> {
                    assertTrue(queryRepositoryAdapter.findByTimestampBucketAndIntervalMinutes(currentBucket, INTERVAL_MINUTES).isPresent());
                    assertTrue(queryRepositoryAdapter.findByTimestampBucketAndIntervalMinutes(pastBucket, INTERVAL_MINUTES).isPresent());
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
}
