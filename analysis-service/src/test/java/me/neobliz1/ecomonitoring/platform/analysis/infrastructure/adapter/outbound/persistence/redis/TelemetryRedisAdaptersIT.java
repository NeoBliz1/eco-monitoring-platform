package me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.persistence.redis;

import static me.neobliz1.ecomonitoring.platform.analysis.domain.model.AnalysisConstants.WEATHER_HOTWINDOW;

import me.neobliz1.ecomonitoring.platform.analysis.infrastructure.config.RedisTestConfig;
import me.neobliz1.ecomonitoring.platform.model.exception.WeatherMapDataNotFoundException;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.RedisSerializer;
import redis.embedded.RedisServer;

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
                        "spring.redis.records.ttl=1"
                )
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

    @Test
    void shouldSuccessfullyWriteAndScanBinaryData_whenValidInputsAreProvided() {
        contextRunner.run(context -> {
            TelemetryPersistenceRepositoryAdapter persistenceRepository = context.getBean(TelemetryPersistenceRepositoryAdapter.class);
            TelemetryQueryRepositoryAdapter queryRepository = context.getBean(TelemetryQueryRepositoryAdapter.class);

            persistenceRepository.saveHistoricalGridCell(GEOHASH, EXPECTED_PAYLOAD);
            Map<String, byte[]> resultsMatrix = queryRepository.findFilteredGridDataBySpatialBox(ACTIVE_BUCKET_FLOOR,
                    MIN_LAT, MAX_LAT, MIN_LON, MAX_LON);

            Assertions.assertNotNull(resultsMatrix);
            Assertions.assertFalse(resultsMatrix.isEmpty());
            Assertions.assertTrue(resultsMatrix.containsKey(GEOHASH));
            Assertions.assertArrayEquals(EXPECTED_PAYLOAD, resultsMatrix.get(GEOHASH));
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
                        Assertions.assertEquals(TIMESTAMP, actualTimestamp);
                    });
        });
    }

    @Test
    void shouldThrowException_whenQueryingEmptyOrNonExistentBucketFloor() {
        contextRunner.run(context -> {
            TelemetryQueryRepositoryAdapter queryRepository = context.getBean(TelemetryQueryRepositoryAdapter.class);

            Assertions.assertThrows(WeatherMapDataNotFoundException.class,
                    () -> queryRepository.findFilteredGridDataBySpatialBox(0L, MIN_LAT, MAX_LAT, MIN_LON, MAX_LON));
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
                    Assertions.assertDoesNotThrow(() -> persistenceRepository.saveRealTimeSlidingWindow(GEOHASH, STATION, TIMESTAMP));
                });
    }
}