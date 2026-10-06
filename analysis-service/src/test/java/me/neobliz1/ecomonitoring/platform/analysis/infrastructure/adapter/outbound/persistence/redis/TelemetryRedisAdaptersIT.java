package me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.persistence.redis;

import static me.neobliz1.ecomonitoring.platform.analysis.domain.model.AnalysisConstants.WEATHER_HOTWINDOW;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import me.neobliz1.ecomonitoring.platform.analysis.domain.model.dto.WeatherMapAnalysisRequestQuery;
import me.neobliz1.ecomonitoring.platform.analysis.domain.service.AnalysisUtils;
import me.neobliz1.ecomonitoring.platform.analysis.infrastructure.config.RedisTestConfig;
import me.neobliz1.ecomonitoring.platform.model.exception.WeatherMapDataNotFoundException;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.RedisSerializer;
import redis.embedded.RedisServer;
import weather.history.HistoryServiceGrpc;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;

class TelemetryRedisAdaptersIT {

    private static final String GEOHASH = "00001787649600000#55.1#-61.3";
    private static final byte[] EXPECTED_PAYLOAD = "mock-binary-protobuf-payload".getBytes(StandardCharsets.UTF_8);
    private static final String STATION = "STATION_001";
    private static final String TIMESTAMP = "2026-08-18T12:00:00Z";
    private static final long ACTIVE_BUCKET_FLOOR = 1787649600000L;
    private static final double MIN_LAT = 55.0;
    private static final double MAX_LAT = 56.0;
    private static final double MIN_LON = -62.0;
    private static final double MAX_LON = -60.0;

    private static RedisServer embeddedRedisProcess;
    private static ApplicationContextRunner contextRunner;

    @BeforeAll
    static void setupSuite() throws IOException {
        embeddedRedisProcess = new RedisServer(6379);
        embeddedRedisProcess.start();

        contextRunner = new ApplicationContextRunner()
                .withPropertyValues(
                        "spring.main.web-application-type=none",
                        "spring.redis.records.ttl=1",
                        "spring.kafka.service-name=analysis-test",
                        "spring.kafka.topic.weather-live=test.weather.live",
                        "spring.kafka.topic.weather-raw=test.weather.raw",
                        "spring.kafka.topic.weather-history=test.weather.history",
                        "spring.kafka.streams.pipeline.name.aggregation-processor.interval=60",
                        "spring.kafka.streams.pipeline.name.deduplication-processor.interval=300000",
                        "spring.kafka.streams.properties.schema.registry.url=http://localhost:8081"
                )
                .withBean(HistoryServiceGrpc.HistoryServiceBlockingStub.class, () ->
                        Mockito.mock(HistoryServiceGrpc.HistoryServiceBlockingStub.class))
                .withBean(LettuceConnectionFactory.class, () -> {
                    RedisStandaloneConfiguration config = new RedisStandaloneConfiguration("localhost", 6379);
                    config.setPassword("testpassword");
                    LettuceConnectionFactory factory = new LettuceConnectionFactory(config);
                    factory.afterPropertiesSet();
                    return factory;
                })
                .withBean("protobufRedisTemplate", RedisTemplate.class, () -> {
                    LettuceConnectionFactory factory = new LettuceConnectionFactory(
                            new RedisStandaloneConfiguration("localhost", 6379));
                    factory.afterPropertiesSet();
                    RedisTemplate<String, byte[]> template = new RedisTemplate<>();
                    template.setConnectionFactory(factory);
                    template.setKeySerializer(RedisSerializer.string());
                    template.setValueSerializer(RedisSerializer.byteArray());
                    template.setHashKeySerializer(RedisSerializer.string());
                    template.setHashValueSerializer(RedisSerializer.byteArray());
                    template.afterPropertiesSet();
                    return template;
                })
                .withBean(ReactiveStringRedisTemplate.class, () -> {
                    LettuceConnectionFactory factory = new LettuceConnectionFactory(
                            new RedisStandaloneConfiguration("localhost", 6379));
                    factory.afterPropertiesSet();
                    return new ReactiveStringRedisTemplate(factory);
                })
                .withUserConfiguration(RedisTestConfig.class);
    }

    @AfterAll
    static void tearDownSuite() throws IOException {
        if(embeddedRedisProcess!=null) {
            embeddedRedisProcess.stop();
        }
    }

    @BeforeEach
    void clearDatabase() {
        contextRunner.run(context -> {
            RedisTemplate<?, ?> template = context.getBean("protobufRedisTemplate", RedisTemplate.class);
            template.getRequiredConnectionFactory().getConnection().serverCommands().flushDb();
        });
    }

    private static WeatherMapAnalysisRequestQuery getQuery(long timestamp) {
        return new WeatherMapAnalysisRequestQuery(timestamp, MIN_LAT, MAX_LAT, MIN_LON, MAX_LON);
    }

    @Test
    void shouldSuccessfullyWriteAndScanBinaryData_whenValidInputsAreProvided() {
        contextRunner.run(context -> {
            TelemetryPersistenceRepositoryAdapter persistenceRepository = context.getBean(TelemetryPersistenceRepositoryAdapter.class);
            TelemetryQueryRepositoryAdapter queryRepository = context.getBean(TelemetryQueryRepositoryAdapter.class);

            persistenceRepository.saveHistoricalGridCell(GEOHASH, EXPECTED_PAYLOAD);
            Map<String, byte[]> resultsMatrix = queryRepository.findFilteredGridDataBySpatialBoxInRedis(
                    getQuery(ACTIVE_BUCKET_FLOOR));

            assertNotNull(resultsMatrix);
            assertFalse(resultsMatrix.isEmpty());
            assertTrue(resultsMatrix.containsKey(GEOHASH));
            assertArrayEquals(EXPECTED_PAYLOAD, resultsMatrix.get(GEOHASH));
        });
    }

    @Test
    void shouldAsynchronouslyPersistSlidingWindowMetrics_whenInvokedWithValidDiagnosticPayload() {
        contextRunner.run(context -> {
            TelemetryPersistenceRepositoryAdapter persistenceRepository = context.getBean(TelemetryPersistenceRepositoryAdapter.class);
            RedisTemplate<String, String> stringTemplate = new RedisTemplate<>();
            stringTemplate.setConnectionFactory(context.getBean(LettuceConnectionFactory.class));
            stringTemplate.setKeySerializer(RedisSerializer.string());
            stringTemplate.setHashKeySerializer(RedisSerializer.string());
            stringTemplate.setHashValueSerializer(RedisSerializer.string());
            stringTemplate.afterPropertiesSet();

            persistenceRepository.saveRealTimeSlidingWindow(GEOHASH, STATION, TIMESTAMP);

            Awaitility.await()
                    .atMost(Duration.ofSeconds(2))
                    .pollInterval(Duration.ofMillis(50))
                    .untilAsserted(() -> {
                        Object actualTimestamp = stringTemplate.opsForHash().get(WEATHER_HOTWINDOW+GEOHASH, STATION);
                        assertEquals(TIMESTAMP, actualTimestamp);
                    });
        });
    }

    @Test
    void shouldThrowException_whenQueryingEmptyOrNonExistentBucketFloor() {
        contextRunner.run(context -> {
            TelemetryQueryRepositoryAdapter queryRepository = context.getBean(TelemetryQueryRepositoryAdapter.class);

            assertThrows(WeatherMapDataNotFoundException.class,
                    () -> queryRepository.getWeatherMapByTimestampAndSpatialBox(
                            getQuery(3600001L)));
        });
    }

    @Test
    void shouldGracefullyRecoverAndLogWarning_whenAsynchronousSlidingWindowFailsDueToMissingDatabaseConnection() {
        contextRunner
                .withAllowBeanDefinitionOverriding(true)
                .withBean("brokenLettuceConnectionFactory", LettuceConnectionFactory.class, () -> {
                    RedisStandaloneConfiguration config = new RedisStandaloneConfiguration("localhost", 9999);
                    LettuceConnectionFactory factory = new LettuceConnectionFactory(config);
                    factory.afterPropertiesSet();
                    return factory;
                })
                .withBean(ReactiveStringRedisTemplate.class, () -> {
                    LettuceConnectionFactory brokenFactory = new LettuceConnectionFactory(new RedisStandaloneConfiguration("localhost", 9999));
                    brokenFactory.afterPropertiesSet();
                    return new ReactiveStringRedisTemplate(brokenFactory);
                })
                .run(context -> {
                    TelemetryPersistenceRepositoryAdapter persistenceRepository = context.getBean(TelemetryPersistenceRepositoryAdapter.class);
                    assertDoesNotThrow(() -> persistenceRepository.saveRealTimeSlidingWindow(GEOHASH, STATION, TIMESTAMP));
                });
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldExtendExpirationTimeToSlidingWindow_whenDataIsAccessedViaSpatialBoxQuery() {
        contextRunner.run(context -> {
            TelemetryPersistenceRepositoryAdapter persistenceRepository = context.getBean(TelemetryPersistenceRepositoryAdapter.class);
            TelemetryQueryRepositoryAdapter queryRepository = context.getBean(TelemetryQueryRepositoryAdapter.class);
            RedisTemplate<String, byte[]> template = context.getBean("protobufRedisTemplate", RedisTemplate.class);
            persistenceRepository.saveHistoricalGridCell(GEOHASH, EXPECTED_PAYLOAD);
            String spatialIndexKey = AnalysisUtils.addSpatialIndexPrefixToTargetFormattedTimestamp(ACTIVE_BUCKET_FLOOR);
            Long initialCellTtl = template.getExpire(GEOHASH);
            Long initialIndexTtl = template.getExpire(spatialIndexKey);
            assertNotNull(initialCellTtl);
            assertTrue(initialCellTtl>0);
            Thread.sleep(1500);

            Map<String, byte[]> resultsMatrix = queryRepository.findFilteredGridDataBySpatialBoxInRedis(
                    getQuery(ACTIVE_BUCKET_FLOOR));

            assertFalse(resultsMatrix.isEmpty());
            Long postAccessCellTtl = template.getExpire(GEOHASH);
            Long postAccessIndexTtl = template.getExpire(spatialIndexKey);
            assertTrue(postAccessCellTtl>=initialCellTtl);
            assertTrue(postAccessIndexTtl>=initialIndexTtl);
        });
    }

    @Test
    void shouldPassivelyEvictRecordsFromCache_whenNoDataAccessOccursWithinTtlWindow() {
        contextRunner
                .withPropertyValues("spring.redis.records.ttl=0")
                .run(context -> {
                    TelemetryPersistenceRepositoryAdapter persistenceRepository = context.getBean(TelemetryPersistenceRepositoryAdapter.class);
                    TelemetryQueryRepositoryAdapter queryRepository = context.getBean(TelemetryQueryRepositoryAdapter.class);
                    persistenceRepository.saveHistoricalGridCell(GEOHASH, EXPECTED_PAYLOAD);

                    Awaitility.await()
                            .atMost(Duration.ofSeconds(3))
                            .pollInterval(Duration.ofMillis(200))
                            .untilAsserted(() -> assertThrows(WeatherMapDataNotFoundException.class,
                                    () -> queryRepository.getWeatherMapByTimestampAndSpatialBox(getQuery(ACTIVE_BUCKET_FLOOR)))
                            );
                });
    }
}