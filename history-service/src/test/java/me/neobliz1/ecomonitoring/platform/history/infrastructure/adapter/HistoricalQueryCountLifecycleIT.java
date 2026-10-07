package me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter;

import static me.neobliz1.ecomonitoring.platform.common.constant.PlatformConstants.COMMON_PROFILE;
import static me.neobliz1.ecomonitoring.platform.common.constant.PlatformConstants.DEV_PROFILE;
import static me.neobliz1.ecomonitoring.platform.common.constant.PlatformConstants.LOCAL_PROFILE;
import static me.neobliz1.ecomonitoring.platform.history.domain.model.constant.HistoricalCacheConstants.BUCKETS_GLOBAL_REGION;
import static me.neobliz1.ecomonitoring.platform.history.domain.model.constant.HistoricalCacheConstants.GRID_CELL_LAYERS_L2_REGION;
import static me.neobliz1.ecomonitoring.platform.history.domain.port.service.HistoricalUtils.getBucketIdFromWeatherMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.springframework.transaction.TransactionDefinition.PROPAGATION_REQUIRES_NEW;

import jakarta.persistence.EntityManagerFactory;
import me.neobliz1.ecomonitoring.platform.history.domain.model.entity.WeatherMapBucket;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.WeatherMap;
import me.neobliz1.ecomonitoring.platform.test.common.util.WeatherTestUtils;
import org.hibernate.SessionFactory;
import org.hibernate.cache.spi.access.EntityDataAccess;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.stat.Statistics;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.redisson.api.RMapCache;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.Cache;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.UUID;

@ActiveProfiles(
        value = { DEV_PROFILE, COMMON_PROFILE, LOCAL_PROFILE },
        inheritProfiles = false
)
public class HistoricalQueryCountLifecycleIT extends IntegrationTestSupport {

    @Autowired
    private SessionFactory sessionFactory;
    @Autowired
    private RedissonClient redissonClient;
    @Autowired
    private EntityManagerFactory entityManagerFactory;
    @Autowired
    private TransactionTemplate transactionTemplate;
    private Statistics stats;

    @BeforeEach
    void setUp() {
        entityManagerFactory.getCache().evictAll();
        stats = sessionFactory.getStatistics();
        stats.clear();
    }

    @Test
    void shouldMakeExactlyFourDatabasePreparedStatements_whenL1AndL2CachesAreCompletelyColdAndBucketHasMultipleLayers() {
        WeatherMap weatherMap = WeatherTestUtils.getWeatherMapWithNLayers(100);
        UUID calculatedId = getBucketIdFromWeatherMap(weatherMap);
        Cache springCache = springL1CacheManager.getCache(BUCKETS_GLOBAL_REGION);
        assertNotNull(springCache);
        springCache.evict(calculatedId);
        RMapCache<Object, Object> bucketsRegionCache = redissonClient.getMapCache(BUCKETS_GLOBAL_REGION);
        bucketsRegionCache.clear();

        adapter.persistTelemetryRecord(weatherMap);

        assertEquals(101, stats.getEntityInsertCount());
        assertEquals(4, stats.getPrepareStatementCount());
    }

    @Test
    void shouldMakeExactlyTwoDatabasePreparedStatements_whenL1CacheHitOnBucketButCellsMustBeLoadedFromDatabase() {
        WeatherMap weatherMap = WeatherTestUtils.getWeatherMap();
        UUID calculatedId = getBucketIdFromWeatherMap(weatherMap);
        transactionTemplate.setPropagationBehavior(PROPAGATION_REQUIRES_NEW);
        transactionTemplate.executeWithoutResult(status -> {
            WeatherMapBucket bucket = new WeatherMapBucket();
            bucket.setId(calculatedId);
            bucket.setTimestampBucket(weatherMap.getTimestampBucket());
            bucket.setIntervalMinutes(weatherMap.getIntervalMinutes());
            queryJpaRepositoryAdapter.saveAndFlush(bucket);
        });
        stats.clear();

        adapter.persistTelemetryRecord(weatherMap);

        assertEquals(1, stats.getPrepareStatementCount()-stats.getEntityInsertCount());
        assertEquals(1, stats.getEntityInsertCount());
        assertEquals(2, stats.getPrepareStatementCount());
    }

    @Test
    void shouldMakeExactlyTwoDatabasePreparedStatements_whenL2CacheHitOnBucketButCellsMustBeLoadedFromDatabase() {
        WeatherMap weatherMap = WeatherTestUtils.getWeatherMap();
        UUID calculatedId = getBucketIdFromWeatherMap(weatherMap);

        adapter.persistTelemetryRecord(weatherMap);

        Cache springCache = springL1CacheManager.getCache(BUCKETS_GLOBAL_REGION);
        assertNotNull(springCache);
        springCache.evict(calculatedId);
        assertNull(springCache.get(calculatedId));
        RMapCache<Object, Object> bucketsRegionCache = redissonClient.getMapCache(BUCKETS_GLOBAL_REGION);
        assertEquals(1, bucketsRegionCache.size());
        stats.clear();

        adapter.persistTelemetryRecord(weatherMap);

        assertEquals(2, stats.getPrepareStatementCount());
        assertEquals(0, stats.getEntityInsertCount());
        assertEquals(1, stats.getEntityUpdateCount());
    }

    @Test
    void shouldPopulateBucketsRegionInL2Cache_whenBucketIsPersisted() {
        WeatherMap weatherMap = WeatherTestUtils.getWeatherMap();
        UUID calculatedId = getBucketIdFromWeatherMap(weatherMap);
        Cache springCache = springL1CacheManager.getCache(BUCKETS_GLOBAL_REGION);
        assertNotNull(springCache);
        assertNull(springCache.get(calculatedId));

        adapter.persistTelemetryRecord(weatherMap);
        RMapCache<Object, Object> bucketsRegionCache = redissonClient.getMapCache(BUCKETS_GLOBAL_REGION);

        assertEquals(1, bucketsRegionCache.size());
    }

    @Test
    void shouldPopulateMetricsRegionInL2Cache_whenBucketIsPersisted() {
        WeatherMap weatherMap = WeatherTestUtils.getWeatherMap();
        UUID calculatedId = getBucketIdFromWeatherMap(weatherMap);
        Cache springCache = springL1CacheManager.getCache(BUCKETS_GLOBAL_REGION);
        assertNotNull(springCache);
        assertNull(springCache.get(calculatedId));

        adapter.persistTelemetryRecord(weatherMap);
        RMapCache<Object, Object> metricsRegionCache = redissonClient.getMapCache(GRID_CELL_LAYERS_L2_REGION);

        assertEquals(1, metricsRegionCache.size());
    }

    @Test
    void shouldReturnNullFromL2Cache_whenBucketIsEvictedFromBothLevels() {
        WeatherMap weatherMap = WeatherTestUtils.getWeatherMap();
        UUID calculatedId = getBucketIdFromWeatherMap(weatherMap);
        adapter.persistTelemetryRecord(weatherMap);
        SessionFactoryImplementor sfi = sessionFactory.unwrap(SessionFactoryImplementor.class);
        EntityPersister persister = sfi.getRuntimeMetamodels()
                .getMappingMetamodel()
                .getEntityDescriptor(WeatherMapBucket.class);
        EntityDataAccess cacheAccess = persister.getCacheAccessStrategy();
        SharedSessionContractImplementor sessionImplementor =
                (SharedSessionContractImplementor) entityManager.getDelegate();
        Object cacheKey = cacheAccess.generateCacheKey(
                calculatedId, persister, sfi, sessionImplementor.getTenantIdentifier());
        RMapCache<Object, Object> bucketsRegionCache = redissonClient.getMapCache(BUCKETS_GLOBAL_REGION);
        assertNotNull(bucketsRegionCache.get(cacheKey));
        Cache springCache = springL1CacheManager.getCache(BUCKETS_GLOBAL_REGION);
        assertNotNull(springCache);
        springCache.evict(calculatedId);
        assertNull(springCache.get(calculatedId));

        entityManagerFactory.getCache().evictAll();

        assertNull(bucketsRegionCache.get(cacheKey));
    }
}