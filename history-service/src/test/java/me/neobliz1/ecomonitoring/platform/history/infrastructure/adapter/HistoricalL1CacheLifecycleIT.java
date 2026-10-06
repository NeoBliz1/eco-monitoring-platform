package me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter;

import static me.neobliz1.ecomonitoring.platform.history.domain.model.constant.HistoricalCacheConstants.BUCKETS_GLOBAL_REGION;
import static me.neobliz1.ecomonitoring.platform.history.domain.model.constant.HistoricalCacheConstants.QUERIES_GLOBAL_REGION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import me.neobliz1.ecomonitoring.platform.history.domain.model.dto.WeatherMapBucketCacheDto;
import me.neobliz1.ecomonitoring.platform.history.domain.model.entity.WeatherGridCellLayer;
import me.neobliz1.ecomonitoring.platform.history.domain.model.entity.WeatherMapBucket;
import me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter.outbound.persistence.postgres.HistoricalPersistenceRepositoryAdapter;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.WeatherMap;
import me.neobliz1.ecomonitoring.platform.test.common.util.WeatherTestUtils;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.springframework.cache.Cache;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

class HistoricalL1CacheLifecycleIT extends IntegrationTestSupport {

    @Test
    void shouldPersistDataInPostgresAndPopulateL1Cache_whenCacheAndDatabaseAreCompletelyCold() {
        WeatherMap weatherMap = WeatherTestUtils.getWeatherMap();
        UUID calculatedId = HistoricalPersistenceRepositoryAdapter.getBucketId(weatherMap);
        Cache springCache = springL1CacheManager.getCache(BUCKETS_GLOBAL_REGION);
        assertNotNull(springCache);

        adapter.persistTelemetryRecord(weatherMap);

        Optional<WeatherMapBucket> dbBucket = queryJpaRepositoryAdapter.findById(calculatedId);
        assertTrue(dbBucket.isPresent());
        assertEquals(weatherMap.getTimestampBucket(), dbBucket.get().getTimestampBucket());
        List<WeatherGridCellLayer> cells = metricsJpaRepository.findSpecificGridCellLayersForMerge(calculatedId, Collections.singleton(WeatherTestUtils.GEOHASH_ALPHA));
        assertEquals(1, cells.size());
        assertEquals(WeatherTestUtils.VAL_COUNT, cells.getFirst().getReadingCount());
        WeatherMapBucketCacheDto cachedDto = springCache.get(calculatedId, WeatherMapBucketCacheDto.class);
        assertNotNull(cachedDto);
        assertEquals(calculatedId, cachedDto.id());
        assertEquals(weatherMap.getTimestampBucket(), cachedDto.timestampBucket());
    }

    @Test
    void shouldReadDirectlyFromL1CacheAndSkipDatabaseBucketFetch_whenCacheHitOccurs() {
        WeatherMap weatherMap = WeatherTestUtils.getWeatherMap();
        UUID calculatedId = HistoricalPersistenceRepositoryAdapter.getBucketId(weatherMap);
        WeatherMapBucket bucket = new WeatherMapBucket();
        bucket.setId(calculatedId);
        bucket.setTimestampBucket(weatherMap.getTimestampBucket());
        bucket.setIntervalMinutes(weatherMap.getIntervalMinutes());
        queryJpaRepositoryAdapter.saveAndFlush(bucket);
        Cache springCache = springL1CacheManager.getCache(BUCKETS_GLOBAL_REGION);
        assertNotNull(springCache);
        WeatherMapBucketCacheDto directDto = new WeatherMapBucketCacheDto(calculatedId, weatherMap.getTimestampBucket(), weatherMap.getIntervalMinutes(), 0);
        springCache.put(calculatedId, directDto);

        adapter.persistTelemetryRecord(weatherMap);

        List<WeatherGridCellLayer> cells = metricsJpaRepository.findSpecificGridCellLayersForMerge(calculatedId, Collections.singleton(WeatherTestUtils.GEOHASH_ALPHA));
        assertEquals(1, cells.size());
        assertEquals(WeatherTestUtils.VAL_COUNT, cells.getFirst().getReadingCount());
    }

    @Test
    void shouldClearQueriesCacheRegion_whenIncomingTelemetryModifiesAnExistingBucketRecord() {
        WeatherMap weatherMap = WeatherTestUtils.getWeatherMap();
        UUID calculatedId = HistoricalPersistenceRepositoryAdapter.getBucketId(weatherMap);
        WeatherMapBucket bucket = new WeatherMapBucket();
        bucket.setId(calculatedId);
        bucket.setTimestampBucket(weatherMap.getTimestampBucket());
        bucket.setIntervalMinutes(weatherMap.getIntervalMinutes());
        queryJpaRepositoryAdapter.saveAndFlush(bucket);
        Cache queryCache = springL1CacheManager.getCache(QUERIES_GLOBAL_REGION);
        assertNotNull(queryCache);
        queryCache.put("grpc-query-key-xyz", "some-cached-grpc-response-payload");

        adapter.persistTelemetryRecord(weatherMap);

        assertNull(queryCache.get("grpc-query-key-xyz"));
    }

    @RepeatedTest(value = 6, name = "Run {currentRepetition} of {totalRepetitions} - Resolving Concurrent Bucket Collisions")
    void shouldSuccessfullyResolveBucketCollisions_whenMultipleParallelThreadsPersistSameTimestampConcurrently() throws Exception {
        int threadCount = 6;
        long sharedTimestampBucket = Instant.now().toEpochMilli();
        int intervalMinutes = 10;
        UUID expectedBucketId = HistoricalPersistenceRepositoryAdapter.getBucketId(
                WeatherTestUtils.getCustomWeatherMap(sharedTimestampBucket, intervalMinutes, "0.0#0.0", 20.0f)
        );
        List<Future<?>> futures;
        boolean executedCleanly;
        try(ExecutorService executorService = Executors.newFixedThreadPool(threadCount)) {
            futures = new ArrayList<>();
            for(int i = 0; i<threadCount; i++) {
                final String uniqueGeohash = String.format("%.1f#%.1f", 40.0+i, -74.0-i);
                final float temperature = 15.0f+i;
                WeatherMap parallelPayload = WeatherTestUtils.getCustomWeatherMap(
                        sharedTimestampBucket,
                        intervalMinutes,
                        uniqueGeohash,
                        temperature
                );
                futures.add(executorService.submit(() -> adapter.persistTelemetryRecord(parallelPayload)));
            }

            executorService.shutdown();
            executedCleanly = executorService.awaitTermination(10, TimeUnit.SECONDS);
        }

        assertTrue(executedCleanly, "Concurrency test timed out! Threads choked or deadlocked.");
        for(Future<?> future : futures) {
            future.get();
        }
        Optional<WeatherMapBucket> dbBucketOpt = queryJpaRepositoryAdapter.findById(expectedBucketId);
        long persistedCellsCount = metricsJpaRepository.count();
        assertTrue(dbBucketOpt.isPresent(), "The target WeatherMapBucket was missing from PostgreSQL");
        WeatherMapBucket dbBucket = dbBucketOpt.get();
        assertEquals(sharedTimestampBucket, dbBucket.getTimestampBucket());
        assertEquals(intervalMinutes, dbBucket.getIntervalMinutes());
        assertTrue(persistedCellsCount>=threadCount, "Some telemetry grid cells were swallowed during the bucket collision!");
    }
}