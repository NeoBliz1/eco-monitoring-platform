package me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter.outbound.persistence.postgres;

import static me.neobliz1.ecomonitoring.platform.history.domain.model.constant.HistoricalCacheConstants.BUCKETS_GLOBAL_REGION;
import static me.neobliz1.ecomonitoring.platform.history.domain.model.constant.HistoricalCacheConstants.QUERIES_GLOBAL_REGION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import me.neobliz1.ecomonitoring.platform.history.domain.model.dto.WeatherMapBucketCacheDto;
import me.neobliz1.ecomonitoring.platform.history.domain.model.entity.WeatherMapBucket;
import me.neobliz1.ecomonitoring.platform.history.domain.model.entity.WeatherTelemetryDlqRecord;
import me.neobliz1.ecomonitoring.platform.history.domain.port.inbound.HistoricalDataConvertService;
import me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter.outbound.persistence.postgres.jpa.HistoricalWeatherGridCellJpaRepository;
import me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter.outbound.persistence.postgres.jpa.HistoricalWeatherMapJpaRepository;
import me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter.outbound.persistence.postgres.jpa.HistoricalWeatherTelemetryDltJpaRepository;
import me.neobliz1.ecomonitoring.platform.model.exception.L1CacheNotAvailableException;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.GridCellLayers;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.WeatherMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;

import java.util.HashSet;
import java.util.Map;
import java.util.UUID;

@ExtendWith(MockitoExtension.class)
class HistoricalPersistenceRepositoryAdapterTest {

    private static final long TEST_TIMESTAMP = 1700000000L;
    private static final int TEST_INTERVAL = 15;
    private static final UUID EXPECTED_BUCKET_ID =
            UUID.nameUUIDFromBytes((TEST_TIMESTAMP+String.valueOf(TEST_INTERVAL)).getBytes());
    @Mock
    private WeatherMapBucketCreationService weatherMapBucketCreationService;
    @Mock
    private HistoricalWeatherTelemetryDltJpaRepository dltJpaRepository;
    @Mock
    private HistoricalDataConvertService weatherMapConverter;
    @Mock
    private HistoricalWeatherMapJpaRepository jpaRepository;
    @Mock
    private HistoricalTxIdRepositoryAdapter txIdAdapter;
    @Mock
    private CacheManager springL1CacheManager;
    @Mock
    private HistoricalWeatherGridCellJpaRepository gridCellJpaRepository;
    @Mock
    private Cache bucketsCache;
    @Mock
    private Cache queriesCache;
    @InjectMocks
    private HistoricalPersistenceRepositoryAdapter adapter;

    @Test
    void shouldCreateNewBucketAndCacheIt_whenBucketDoesNotExistInCacheOrDatabase() {
        WeatherMap weatherMap = createWeatherMapWithPartiallyMocks();
        setupCacheManagers();
        WeatherMapBucket savedBucket = createBaseBucket(0L);
        doReturn(savedBucket).when(weatherMapBucketCreationService).saveWeatherMapBucket(any(WeatherMap.class));
        ArgumentCaptor<WeatherMapBucketCacheDto> dtoCaptor = ArgumentCaptor.forClass(WeatherMapBucketCacheDto.class);

        adapter.persistTelemetryRecord(weatherMap);

        verify(weatherMapBucketCreationService, times(1)).saveWeatherMapBucket(any(WeatherMap.class));
        verify(bucketsCache).put(eq(EXPECTED_BUCKET_ID), dtoCaptor.capture());
    }

    @Test
    void shouldLoadFromDatabaseAndPutCache_whenBucketMissingFromCacheButExistsInDatabase() {
        WeatherMap weatherMap = createWeatherMapWithPartiallyMocks();
        setupCacheManagers();
        WeatherMapBucket existingDbBucket = createBaseBucket(1L);
        doReturn(existingDbBucket).when(weatherMapBucketCreationService).saveWeatherMapBucket(any(WeatherMap.class));

        adapter.persistTelemetryRecord(weatherMap);

        verify(weatherMapBucketCreationService, times(1)).saveWeatherMapBucket(any(WeatherMap.class));
        verify(weatherMapConverter).mergeTelemetryInBatch(weatherMap, existingDbBucket);
        verify(bucketsCache).put(eq(EXPECTED_BUCKET_ID), any(WeatherMapBucketCacheDto.class));
    }

    @Test
    void shouldLoadProxyFromDatabaseAndAvoidRowSelect_whenBucketExistsDirectlyInL1Cache() {
        WeatherMap weatherMap = createWeatherMapWithPartiallyMocks();
        setupCacheManagers();
        WeatherMapBucketCacheDto cachedDto =
                new WeatherMapBucketCacheDto(EXPECTED_BUCKET_ID, TEST_TIMESTAMP, TEST_INTERVAL, 1);
        doReturn(cachedDto)
                .when(bucketsCache).get(EXPECTED_BUCKET_ID, WeatherMapBucketCacheDto.class);
        WeatherMapBucket proxyBucketPlaceholder = mock(WeatherMapBucket.class);
        doReturn(proxyBucketPlaceholder)
                .when(jpaRepository).getReferenceById(EXPECTED_BUCKET_ID);

        adapter.persistTelemetryRecord(weatherMap);

        verify(jpaRepository, never()).saveAndFlush(any(WeatherMapBucket.class));
        verify(jpaRepository, never()).findById(any(UUID.class));
        verify(jpaRepository, times(1)).getReferenceById(EXPECTED_BUCKET_ID);
        verify(weatherMapConverter).mergeTelemetryInBatch(weatherMap, proxyBucketPlaceholder);
        verify(bucketsCache, never()).put(any(), any());
    }

    @Test
    void shouldSaveDltRecordSuccessfully_whenValidDltPayloadProvided() {
        WeatherTelemetryDlqRecord dltRecord = mock(WeatherTelemetryDlqRecord.class);

        adapter.persistDltRecord(dltRecord);

        verify(dltJpaRepository).saveAndFlush(dltRecord);
    }

    @Test
    void shouldGenerateConsistentUuid_whenStaticGetBucketIdInvoked() {
        WeatherMap weatherMap = mock(WeatherMap.class);
        when(weatherMap.getTimestampBucket()).thenReturn(TEST_TIMESTAMP);
        when(weatherMap.getIntervalMinutes()).thenReturn(TEST_INTERVAL);

        UUID generatedId = HistoricalPersistenceRepositoryAdapter.getBucketId(weatherMap);

        assertNotNull(generatedId);
        assertEquals(UUID.nameUUIDFromBytes("170000000015".getBytes()), generatedId);
    }

    @Test
    void shouldThrowL1CacheNotAvailableException_whenBucketsCacheIsNull() {
        WeatherMap weatherMap = createWeatherMapWithPartiallyMocks();
        doReturn(null).when(springL1CacheManager).getCache(BUCKETS_GLOBAL_REGION);

        assertThrows(L1CacheNotAvailableException.class, () -> adapter.persistTelemetryRecord(weatherMap));
    }

    @Test
    void shouldThrowL1CacheNotAvailableException_whenQueriesCacheIsNull() {
        Mockito.reset(springL1CacheManager);
        WeatherMap weatherMap = createWeatherMapWithPartiallyMocks();

        assertThrows(L1CacheNotAvailableException.class, () -> adapter.persistTelemetryRecord(weatherMap));
    }

    @Test
    void shouldPropagateException_whenDltJpaRepositoryFailsToSave() {
        WeatherTelemetryDlqRecord dltRecord = mock(WeatherTelemetryDlqRecord.class);
        RuntimeException databaseException = new RuntimeException("Database offline");
        doThrow(databaseException).when(dltJpaRepository).saveAndFlush(dltRecord);

        RuntimeException thrown = assertThrows(RuntimeException.class, () -> adapter.persistDltRecord(dltRecord));

        assertEquals("Database offline", thrown.getMessage());
    }

    @SuppressWarnings("unchecked")
    private WeatherMap createMockWeatherMap() {
        WeatherMap weatherMap = mock(WeatherMap.class);
        Map<String, GridCellLayers> mockGridCellsMap = mock(Map.class);
        when(weatherMap.getGridCellsMap()).thenReturn(mockGridCellsMap);
        return weatherMap;
    }

    private WeatherMap createWeatherMapWithPartiallyMocks() {
        WeatherMap weatherMap = createMockWeatherMap();
        when(weatherMap.getTimestampBucket()).thenReturn(TEST_TIMESTAMP);
        when(weatherMap.getIntervalMinutes()).thenReturn(TEST_INTERVAL);
        return weatherMap;
    }

    private void setupCacheManagers() {
        doReturn(bucketsCache).when(springL1CacheManager).getCache(BUCKETS_GLOBAL_REGION);
        doReturn(queriesCache).when(springL1CacheManager).getCache(QUERIES_GLOBAL_REGION);
    }

    private WeatherMapBucket createBaseBucket(Long version) {
        WeatherMapBucket bucket = new WeatherMapBucket();
        bucket.setId(EXPECTED_BUCKET_ID);
        bucket.setTimestampBucket(TEST_TIMESTAMP);
        bucket.setIntervalMinutes(TEST_INTERVAL);
        bucket.setVersion(version);
        bucket.setGridCells(new HashSet<>());
        return bucket;
    }
}