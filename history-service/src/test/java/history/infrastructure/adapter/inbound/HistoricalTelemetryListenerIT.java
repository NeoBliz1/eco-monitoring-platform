package history.infrastructure.adapter.inbound;

import static me.neobliz1.ecomonitoring.platform.test.common.util.WeatherTestUtils.GEOHASH_ALPHA;
import static me.neobliz1.ecomonitoring.platform.test.common.util.WeatherTestUtils.INTERVAL_MINUTES;
import static me.neobliz1.ecomonitoring.platform.test.common.util.WeatherTestUtils.loadEnvironmentMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import me.neobliz1.ecomonitoring.platform.history.domain.model.entity.WeatherGridCellMetric;
import me.neobliz1.ecomonitoring.platform.history.domain.model.entity.WeatherMapBucket;
import me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter.outbound.persistence.postgres.HistoricalQueryJpaRepository;
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

import java.io.File;
import java.time.Duration;
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
    private HistoricalQueryJpaRepository queryRepositoryAdapter;

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
        WeatherMap poisonousMap = WeatherMap.newBuilder()
                .setTimestampBucket(1819234000L)
                .setIntervalMinutes(INTERVAL_MINUTES)
                .putGridCells(GEOHASH_ALPHA, GridCellLayers.newBuilder()
                        .setReadingCount(1)
                        .setAvgHumidity(150.0)
                        .build())
                .build();

        sendPacket(1819234000L, poisonousMap);

        Awaitility.await()
                .atMost(Duration.ofSeconds(10))
                .during(Duration.ofSeconds(3))
                .until(() -> queryRepositoryAdapter
                        .findByTimestampBucketAndIntervalMinutes(1819234000L, INTERVAL_MINUTES)
                        .isEmpty());
        List<WeatherMapBucket> bucketList = queryJpaRepositoryAdapter.findAll();
        assertTrue(bucketList.isEmpty());
    }

    @Test
    void shouldRejectPayloadAndHaltProgression_whenGridCellContainsZeroActualMetricGroupReadings() throws Exception {
        WeatherMap emptyPayload = WeatherMap.newBuilder()
                .setTimestampBucket(1919234000L)
                .setIntervalMinutes(INTERVAL_MINUTES)
                .putGridCells(GEOHASH_ALPHA, GridCellLayers.newBuilder()
                        .setReadingCount(5)
                        .build())
                .build();

        sendPacket(1919234000L, emptyPayload);

        Awaitility.await()
                .atMost(Duration.ofMinutes(1))
                .during(Duration.ofSeconds(3))
                .until(() -> queryRepositoryAdapter
                        .findByTimestampBucketAndIntervalMinutes(1919234000L, INTERVAL_MINUTES)
                        .isEmpty());
        List<WeatherMapBucket> bucketList = queryJpaRepositoryAdapter.findAll();
        assertTrue(bucketList.isEmpty());
    }

    @Test
    void shouldAccumulateDistinctGeohashMetricsIntoSingleBucket_whenTwoDistinctPayloadsArriveSerially() throws Exception {
        long timestamp = 2119234000L;
        WeatherMap alphaPayload = WeatherTestUtils.getCustomWeatherMap(timestamp, GEOHASH_ALPHA, 22.5f);
        String geohash2 = "w21z7";
        WeatherMap betaPayload = WeatherTestUtils.getCustomWeatherMap(timestamp, geohash2, 18.2f);

        sendPacket(timestamp, alphaPayload);
        sendPacket(timestamp, betaPayload);
        Awaitility.await()
                .atMost(Duration.ofSeconds(15))
                .pollInterval(Duration.ofMillis(300))
                .untilAsserted(() -> {
                    WeatherMapBucket weatherMapBucket = getWeatherMapBucket(timestamp);
                    assertEquals(2, weatherMapBucket.getGridCells().size());
                });
        WeatherMapBucket targetBucket = getWeatherMapBucket(timestamp);
        long bucketRowCount = queryJpaRepositoryAdapter.count();
        List<String> persistedGeohashes = targetBucket.getGridCells().stream()
                .map(WeatherGridCellMetric::getGeohash)
                .toList();

        assertEquals(1L, bucketRowCount);
        assertTrue(persistedGeohashes.contains(GEOHASH_ALPHA));
        assertTrue(persistedGeohashes.contains(geohash2));
    }
}
