package me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter;

import static me.neobliz1.ecomonitoring.platform.common.constant.PlatformConstants.COMMON_PROFILE;
import static me.neobliz1.ecomonitoring.platform.common.constant.PlatformConstants.DEV_PROFILE;
import static me.neobliz1.ecomonitoring.platform.common.constant.PlatformConstants.LOCAL_PROFILE;
import static me.neobliz1.ecomonitoring.platform.history.domain.model.constant.HistoricalCacheConstants.BUCKETS_REGION;
import static me.neobliz1.ecomonitoring.platform.history.domain.model.constant.HistoricalCacheConstants.METRICS_REGION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.persistence.EntityManagerFactory;
import me.neobliz1.ecomonitoring.platform.history.domain.model.entity.WeatherMapBucket;
import me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter.outbound.persistence.postgres.HistoricalPersistenceRepositoryAdapter;
import me.neobliz1.ecomonitoring.platform.history.infrastructure.config.HistoryInfrastructureProperties;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.WeatherMap;
import me.neobliz1.ecomonitoring.platform.test.common.util.WeatherTestUtils;
import org.awaitility.Awaitility;
import org.hibernate.SessionFactory;
import org.hibernate.cache.spi.access.EntityDataAccess;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.stat.CacheRegionStatistics;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.redisson.api.RMapCache;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.redisson.config.SingleServerConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.data.redis.connection.RedisPassword;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.test.context.ActiveProfiles;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Import(HistoricalL2CacheLifecycleIT.RedisTestConfig.class)
@ActiveProfiles(
        value = { DEV_PROFILE, COMMON_PROFILE, LOCAL_PROFILE },
        inheritProfiles = false
)
public class HistoricalL2CacheLifecycleIT extends IntegrationTestSupport {

    private static final int CONFIGURED_BUCKETS_MAX_ENTRIES = 4;

    @Autowired
    private EntityManagerFactory entityManagerFactory;
    @Autowired
    private SessionFactory sessionFactory;
    @Autowired
    private RedisTemplate<Object, Object> redisTemplate;
    @Autowired
    private RedissonClient redissonClient;

    private static @NonNull List<WeatherMap> getWeatherMaps() {
        long baseTimestamp = System.currentTimeMillis();
        WeatherMap weatherMapOne = WeatherMap.newBuilder()
                .setTimestampBucket(baseTimestamp)
                .setIntervalMinutes(WeatherTestUtils.INTERVAL_MINUTES)
                .addTelemetryTransactionsId(WeatherTestUtils.TX_DEFAULT_ID)
                .putGridCells(WeatherTestUtils.GEOHASH_ALPHA, WeatherTestUtils.getGridCellLayers(null))
                .build();
        long timestampTwo = baseTimestamp+86400000L;
        WeatherMap weatherMapTwo = WeatherMap.newBuilder()
                .setTimestampBucket(timestampTwo)
                .setIntervalMinutes(WeatherTestUtils.INTERVAL_MINUTES)
                .addTelemetryTransactionsId(WeatherTestUtils.TX_DEFAULT_ID)
                .putGridCells(WeatherTestUtils.GEOHASH_ALPHA, WeatherTestUtils.getGridCellLayers(null))
                .build();
        long timestampThree = baseTimestamp+172800000L;
        WeatherMap weatherMapThree = WeatherMap.newBuilder()
                .setTimestampBucket(timestampThree)
                .setIntervalMinutes(WeatherTestUtils.INTERVAL_MINUTES)
                .addTelemetryTransactionsId(WeatherTestUtils.TX_DEFAULT_ID)
                .putGridCells(WeatherTestUtils.GEOHASH_ALPHA, WeatherTestUtils.getGridCellLayers(null))
                .build();
        return List.of(weatherMapOne, weatherMapTwo, weatherMapThree);
    }

    private static @NonNull List<WeatherMap> getWeatherMapsWithDistinctBuckets() {
        long baseTimestamp = System.currentTimeMillis();
        List<WeatherMap> weatherMapList = new ArrayList<>(5);
        for(int i = 0; i<5; i++) {
            long timestamp = baseTimestamp+(86400000L*i);
            WeatherMap weatherMap = WeatherMap.newBuilder()
                    .setTimestampBucket(timestamp)
                    .setIntervalMinutes(WeatherTestUtils.INTERVAL_MINUTES)
                    .addTelemetryTransactionsId(WeatherTestUtils.TX_DEFAULT_ID)
                    .putGridCells(WeatherTestUtils.GEOHASH_ALPHA, WeatherTestUtils.getGridCellLayers(null))
                    .build();
            weatherMapList.add(weatherMap);
        }
        return weatherMapList;
    }

    @BeforeEach
    void setUp() {
        super.setupEcosystem();
        entityManagerFactory.getCache().evictAll();
        sessionFactory.getStatistics().clear();
    }

    @Test
    void shouldExtractThreeHibernateCacheKeysAndRetrieveRawPayloadsFromRedisTemplate_whenThreeRecordsArePersisted() {
        List<WeatherMap> weatherMapList = getWeatherMaps();

        adapter.persistTelemetryRecord(weatherMapList.get(0));
        adapter.persistTelemetryRecord(weatherMapList.get(1));
        adapter.persistTelemetryRecord(weatherMapList.get(2));
        byte[] rawHashRegionKey = BUCKETS_REGION.getBytes(StandardCharsets.UTF_8);
        Map<Object, Object> allRedisCacheEntries = redisTemplate.opsForHash().entries(rawHashRegionKey);
        SessionFactoryImplementor sfi = sessionFactory.unwrap(SessionFactoryImplementor.class);
        CacheRegionStatistics regionStats = sfi.getStatistics().getDomainDataRegionStatistics(BUCKETS_REGION);

        assertEquals(3, allRedisCacheEntries.size());
        assertNotNull(regionStats);
        assertEquals(3, regionStats.getPutCount());
    }

    @Test
    void shouldPopulateMetricsCacheRegionWithExactlyThreeElements_whenThreeDistinctRecordsArePersisted() {
        List<WeatherMap> weatherMapList = getWeatherMaps();

        adapter.persistTelemetryRecord(weatherMapList.get(0));
        adapter.persistTelemetryRecord(weatherMapList.get(1));
        adapter.persistTelemetryRecord(weatherMapList.get(2));
        byte[] metricsRegionKey = METRICS_REGION.getBytes(StandardCharsets.UTF_8);
        Map<Object, Object> allRedisMetricsEntries = redisTemplate.opsForHash().entries(metricsRegionKey);
        SessionFactoryImplementor sfi = sessionFactory.unwrap(SessionFactoryImplementor.class);
        CacheRegionStatistics metricsRegionStats = sfi.getStatistics().getDomainDataRegionStatistics(METRICS_REGION);

        assertEquals(3, allRedisMetricsEntries.size());
        assertNotNull(metricsRegionStats);
        assertEquals(3, metricsRegionStats.getPutCount());
    }

    @Test
    void shouldNotExceedConfiguredMaxEntries_whenMoreBucketsThanMaxArePersisted() {
        List<WeatherMap> weatherMapList = getWeatherMapsWithDistinctBuckets();

        for(WeatherMap weatherMap : weatherMapList) {
            adapter.persistTelemetryRecord(weatherMap);
        }
        RMapCache<Object, Object> bucketsRegionCache = redissonClient.getMapCache(BUCKETS_REGION);

        Awaitility.await()
                .atMost(Duration.ofSeconds(10))
                .pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> assertTrue(
                        bucketsRegionCache.size()<=CONFIGURED_BUCKETS_MAX_ENTRIES,
                        "Region size "+bucketsRegionCache.size()
                                +" exceeds configured max "+CONFIGURED_BUCKETS_MAX_ENTRIES));
    }

    @Test
    void shouldEvictOldestBucketFromLru_whenMoreBucketsThanMaxArePersisted() {
        List<WeatherMap> weatherMapList = getWeatherMapsWithDistinctBuckets();
        UUID oldestBucketId = HistoricalPersistenceRepositoryAdapter.getBucketId(weatherMapList.getFirst());

        for(WeatherMap weatherMap : weatherMapList) {
            adapter.persistTelemetryRecord(weatherMap);
        }
        SessionFactoryImplementor sfi = sessionFactory.unwrap(SessionFactoryImplementor.class);
        EntityPersister persister = sfi.getRuntimeMetamodels()
                .getMappingMetamodel()
                .getEntityDescriptor(WeatherMapBucket.class);
        EntityDataAccess cacheAccess = persister.getCacheAccessStrategy();
        SharedSessionContractImplementor sessionImplementor =
                (SharedSessionContractImplementor) entityManager.getDelegate();
        Object oldestCacheKey = cacheAccess.generateCacheKey(
                oldestBucketId, persister, sfi, sessionImplementor.getTenantIdentifier());
        RMapCache<Object, Object> bucketsRegionCache = redissonClient.getMapCache(BUCKETS_REGION);

        Awaitility.await()
                .atMost(Duration.ofSeconds(10))
                .pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> assertNull(bucketsRegionCache.get(oldestCacheKey)));
    }

    @Test
    void shouldEvictL2CacheRecordNativelyFromRedis_whenBucketRecordExpiryThresholdIsReached() {
        List<WeatherMap> weatherMapList = getWeatherMaps();
        WeatherMap targetMap = weatherMapList.getFirst();
        UUID targetId = HistoricalPersistenceRepositoryAdapter.getBucketId(targetMap);

        adapter.persistTelemetryRecord(targetMap);
        SessionFactoryImplementor sfi = sessionFactory.unwrap(SessionFactoryImplementor.class);
        EntityPersister persister = sfi.getRuntimeMetamodels().getMappingMetamodel().getEntityDescriptor(WeatherMapBucket.class);
        EntityDataAccess cacheAccess = persister.getCacheAccessStrategy();
        SharedSessionContractImplementor sessionImplementor = (SharedSessionContractImplementor) entityManager.getDelegate();
        Object cacheKey = cacheAccess.generateCacheKey(targetId, persister, sfi, sessionImplementor.getTenantIdentifier());
        RMapCache<Object, Object> redissonMapCache = redissonClient.getMapCache(BUCKETS_REGION);

        assertNotNull(redissonMapCache.get(cacheKey));

        Awaitility.await()
                .atMost(Duration.ofMinutes(2))
                .pollInterval(Duration.ofSeconds(65))
                .untilAsserted(() -> assertNull(redissonMapCache.get(cacheKey)));
    }

    @TestConfiguration
    static class RedisTestConfig {

        @Autowired
        private HistoryInfrastructureProperties infraProps;

        @Bean
        public LettuceConnectionFactory redisConnectionFactory(RedissonClient redissonClient) {
            Config redissonConfig = redissonClient.getConfig();
            SingleServerConfig singleServerConfig = redissonConfig.useSingleServer();
            String address = singleServerConfig.getAddress();
            String cleanAddress = address.replace("redis://", "").replace("rediss://", "");
            String[] hostAndPort = cleanAddress.split(":");
            String redisHost = hostAndPort[0];
            int redisPort = Integer.parseInt(hostAndPort[1]);
            HistoryInfrastructureProperties.Data.Redis redis = infraProps.getData().getRedis();
            String redisPassword = redis.getPassword();
            RedisStandaloneConfiguration config = new RedisStandaloneConfiguration();
            config.setHostName(redisHost);
            config.setPort(redisPort);

            if(redisPassword!=null && !redisPassword.isBlank()) {
                config.setPassword(RedisPassword.of(redisPassword));
            } else {
                config.setPassword(RedisPassword.none());
            }

            LettuceClientConfiguration clientConfig = LettuceClientConfiguration.builder()
                    .commandTimeout(Duration.ofMillis(200))
                    .shutdownTimeout(Duration.ofMillis(100))
                    .build();

            LettuceConnectionFactory factory = new LettuceConnectionFactory(config, clientConfig);
            factory.setEagerInitialization(true);
            factory.afterPropertiesSet();
            return factory;
        }

        @Bean
        public RedisTemplate<Object, Object> redisTemplate(LettuceConnectionFactory connectionFactory) {
            RedisTemplate<Object, Object> template = new RedisTemplate<>();
            template.setConnectionFactory(connectionFactory);
            template.setKeySerializer(RedisSerializer.byteArray());
            template.setValueSerializer(RedisSerializer.byteArray());
            template.setHashKeySerializer(RedisSerializer.byteArray());
            template.setHashValueSerializer(RedisSerializer.byteArray());
            template.afterPropertiesSet();
            return template;
        }
    }
}