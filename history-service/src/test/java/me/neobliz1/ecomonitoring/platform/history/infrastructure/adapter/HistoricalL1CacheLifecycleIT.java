package me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter;

import static me.neobliz1.ecomonitoring.platform.history.domain.model.constant.HistoricalCacheConstants.BUCKETS_REGION;
import static me.neobliz1.ecomonitoring.platform.history.domain.model.constant.HistoricalCacheConstants.QUERIES_REGION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import me.neobliz1.ecomonitoring.platform.history.domain.model.dto.WeatherMapBucketCacheDto;
import me.neobliz1.ecomonitoring.platform.history.domain.model.entity.WeatherGridCellMetric;
import me.neobliz1.ecomonitoring.platform.history.domain.model.entity.WeatherMapBucket;
import me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter.outbound.persistence.postgres.HistoricalPersistenceRepositoryAdapter;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.WeatherMap;
import me.neobliz1.ecomonitoring.platform.test.common.util.WeatherTestUtils;
import org.junit.jupiter.api.Test;
import org.springframework.cache.Cache;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

class HistoricalL1CacheLifecycleIT extends IntegrationTestSupport {

    @Test
    void shouldPersistDataInPostgresAndPopulateL1Cache_whenCacheAndDatabaseAreCompletelyCold() {
        WeatherMap weatherMap = WeatherTestUtils.getWeatherMap();
        UUID calculatedId = HistoricalPersistenceRepositoryAdapter.getBucketId(weatherMap);
        Cache springCache = springL1CacheManager.getCache(BUCKETS_REGION);
        assertNotNull(springCache);

        adapter.persistTelemetryRecord(weatherMap);

        Optional<WeatherMapBucket> dbBucket = queryJpaRepositoryAdapter.findById(calculatedId);
        assertTrue(dbBucket.isPresent());
        assertEquals(weatherMap.getTimestampBucket(), dbBucket.get().getTimestampBucket());
        List<WeatherGridCellMetric> cells = metricsJpaRepository.findSpecificCellsForMerge(calculatedId, Collections.singleton(WeatherTestUtils.GEOHASH_ALPHA));
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
        bucket.setVersion(0);
        queryJpaRepositoryAdapter.saveAndFlush(bucket);
        Cache springCache = springL1CacheManager.getCache(BUCKETS_REGION);
        assertNotNull(springCache);
        WeatherMapBucketCacheDto directDto = new WeatherMapBucketCacheDto(calculatedId, weatherMap.getTimestampBucket(), weatherMap.getIntervalMinutes(), 0);
        springCache.put(calculatedId, directDto);

        adapter.persistTelemetryRecord(weatherMap);

        List<WeatherGridCellMetric> cells = metricsJpaRepository.findSpecificCellsForMerge(calculatedId, Collections.singleton(WeatherTestUtils.GEOHASH_ALPHA));
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
        Cache queryCache = springL1CacheManager.getCache(QUERIES_REGION);
        assertNotNull(queryCache);
        queryCache.put("grpc-query-key-xyz", "some-cached-grpc-response-payload");

        adapter.persistTelemetryRecord(weatherMap);

        assertNull(queryCache.get("grpc-query-key-xyz"));
    }
}