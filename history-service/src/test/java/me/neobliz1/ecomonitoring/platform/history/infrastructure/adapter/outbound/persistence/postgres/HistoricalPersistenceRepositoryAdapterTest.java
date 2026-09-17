package me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter.outbound.persistence.postgres;

import static me.neobliz1.ecomonitoring.platform.history.domain.model.constant.HistoricalCacheConstants.BUCKETS_REGION;
import static me.neobliz1.ecomonitoring.platform.history.domain.model.constant.HistoricalCacheConstants.QUERIES_REGION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import jakarta.persistence.EntityManager;
import me.neobliz1.ecomonitoring.platform.history.domain.model.dto.WeatherMapBucketCacheDto;
import me.neobliz1.ecomonitoring.platform.history.domain.model.entity.WeatherGridCellMetric;
import me.neobliz1.ecomonitoring.platform.history.domain.model.entity.WeatherMapBucket;
import me.neobliz1.ecomonitoring.platform.history.domain.model.entity.WeatherTelemetryDltRecord;
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
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class HistoricalPersistenceRepositoryAdapterTest {

    private static final long TEST_TIMESTAMP = 1700000000L;
    private static final int TEST_INTERVAL = 15;
    private static final UUID EXPECTED_BUCKET_ID = UUID.nameUUIDFromBytes((String.valueOf(TEST_TIMESTAMP)+TEST_INTERVAL).getBytes());

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
    private EntityManager entityManager;
    @Mock
    private HistoricalWeatherGridCellJpaRepository gridCellRepository;
    @Mock
    private Cache bucketsCache;
    @Mock
    private Cache queriesCache;

    @InjectMocks
    private HistoricalPersistenceRepositoryAdapter adapter;

    @Test
    void shouldCreateNewBucketAndCacheIt_whenBucketDoesNotExistInCacheOrDatabase() {
        WeatherMap weatherMap = createMockWeatherMap(Collections.emptySet(), TEST_INTERVAL);
        setupCacheManagers();
        when(bucketsCache.get(EXPECTED_BUCKET_ID, WeatherMapBucketCacheDto.class)).thenReturn(null);
        when(jpaRepository.findById(EXPECTED_BUCKET_ID)).thenReturn(Optional.empty());
        WeatherMapBucket savedBucket = createBaseBucket(0);
        when(jpaRepository.saveAndFlush(any(WeatherMapBucket.class))).thenReturn(savedBucket);
        ArgumentCaptor<WeatherMapBucket> bucketCaptor = ArgumentCaptor.forClass(WeatherMapBucket.class);
        ArgumentCaptor<WeatherMapBucketCacheDto> dtoCaptor = ArgumentCaptor.forClass(WeatherMapBucketCacheDto.class);

        adapter.persistTelemetryRecord(weatherMap);

        verify(jpaRepository, times(2)).saveAndFlush(bucketCaptor.capture());
        WeatherMapBucket createdBucket = bucketCaptor.getValue();
        assertEquals(EXPECTED_BUCKET_ID, createdBucket.getId());
        assertEquals(TEST_TIMESTAMP, createdBucket.getTimestampBucket());
        assertEquals(TEST_INTERVAL, createdBucket.getIntervalMinutes());
        verify(weatherMapConverter).mergeTelemetryInBatch(eq(weatherMap), eq(createdBucket), anyList());
        verify(bucketsCache).put(eq(EXPECTED_BUCKET_ID), dtoCaptor.capture());
        WeatherMapBucketCacheDto cachedDto = dtoCaptor.getValue();
        assertEquals(EXPECTED_BUCKET_ID, cachedDto.id());
        assertEquals(TEST_TIMESTAMP, cachedDto.timestampBucket());
        verify(txIdAdapter).processTxIdsHistoryBatch(weatherMap);
    }

    @Test
    void shouldLoadFromDatabaseAndPutCache_whenBucketMissingFromCacheButExistsInDatabase() {
        Set<String> targetGeohashes = Collections.singleton("geo123");
        WeatherMap weatherMap = createMockWeatherMap(targetGeohashes, TEST_INTERVAL);
        setupCacheManagers();
        when(bucketsCache.get(EXPECTED_BUCKET_ID, WeatherMapBucketCacheDto.class)).thenReturn(null);
        WeatherMapBucket existingDbBucket = createBaseBucket(1);
        when(jpaRepository.findById(EXPECTED_BUCKET_ID)).thenReturn(Optional.of(existingDbBucket));
        List<WeatherGridCellMetric> mockedTargetedCells = List.of(mock(WeatherGridCellMetric.class));
        when(gridCellRepository.findSpecificCellsForMerge(EXPECTED_BUCKET_ID, targetGeohashes)).thenReturn(mockedTargetedCells);
        when(jpaRepository.saveAndFlush(existingDbBucket)).thenReturn(existingDbBucket);

        adapter.persistTelemetryRecord(weatherMap);

        verify(entityManager, never()).merge(any());
        verify(weatherMapConverter).mergeTelemetryInBatch(weatherMap, existingDbBucket, mockedTargetedCells);
        verify(bucketsCache, never()).evict(any());
        verify(bucketsCache).put(eq(EXPECTED_BUCKET_ID), any(WeatherMapBucketCacheDto.class));
    }

    @Test
    void shouldLoadProxyFromDatabaseAndAvoidRowSelect_whenBucketExistsDirectlyInL1Cache() {
        Set<String> targetGeohashes = Collections.singleton("geo123");
        WeatherMap weatherMap = createMockWeatherMap(targetGeohashes, TEST_INTERVAL);
        setupCacheManagers();
        WeatherMapBucketCacheDto cachedDto = new WeatherMapBucketCacheDto(EXPECTED_BUCKET_ID, TEST_TIMESTAMP, TEST_INTERVAL, 1);
        when(bucketsCache.get(EXPECTED_BUCKET_ID, WeatherMapBucketCacheDto.class)).thenReturn(cachedDto);
        WeatherMapBucket proxyBucketPlaceholder = mock(WeatherMapBucket.class);
        when(proxyBucketPlaceholder.getId()).thenReturn(EXPECTED_BUCKET_ID);
        when(jpaRepository.getReferenceById(EXPECTED_BUCKET_ID)).thenReturn(proxyBucketPlaceholder);
        List<WeatherGridCellMetric> mockedTargetedCells = List.of(mock(WeatherGridCellMetric.class));
        when(gridCellRepository.findSpecificCellsForMerge(EXPECTED_BUCKET_ID, targetGeohashes)).thenReturn(mockedTargetedCells);
        when(jpaRepository.saveAndFlush(proxyBucketPlaceholder)).thenReturn(proxyBucketPlaceholder);

        adapter.persistTelemetryRecord(weatherMap);

        verify(jpaRepository, never()).findById(any(UUID.class));
        verify(entityManager, never()).merge(any());
        verify(weatherMapConverter).mergeTelemetryInBatch(weatherMap, proxyBucketPlaceholder, mockedTargetedCells);
        verify(bucketsCache).put(eq(EXPECTED_BUCKET_ID), any(WeatherMapBucketCacheDto.class));
    }

    @Test
    void shouldSaveDltRecordSuccessfully_whenValidDltPayloadProvided() {
        WeatherTelemetryDltRecord dltRecord = mock(WeatherTelemetryDltRecord.class);

        adapter.persistDltRecord(dltRecord);

        verify(dltJpaRepository).saveAndFlush(dltRecord);
    }

    @Test
    void shouldThrowL1CacheNotAvailableException_whenBucketsCacheIsNull() {
        WeatherMap weatherMap = mock(WeatherMap.class);
        when(springL1CacheManager.getCache(BUCKETS_REGION)).thenReturn(null);
        when(springL1CacheManager.getCache(QUERIES_REGION)).thenReturn(queriesCache);

        assertThrows(L1CacheNotAvailableException.class, () -> adapter.persistTelemetryRecord(weatherMap));
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
    void shouldThrowL1CacheNotAvailableException_whenQueriesCacheIsNull() {
        WeatherMap weatherMap = mock(WeatherMap.class);
        when(springL1CacheManager.getCache(BUCKETS_REGION)).thenReturn(bucketsCache);
        when(springL1CacheManager.getCache(QUERIES_REGION)).thenReturn(null);

        assertThrows(L1CacheNotAvailableException.class, () -> adapter.persistTelemetryRecord(weatherMap));
    }

    @Test
    void shouldPropagateException_whenDltJpaRepositoryFailsToSave() {
        WeatherTelemetryDltRecord dltRecord = mock(WeatherTelemetryDltRecord.class);
        RuntimeException databaseException = new RuntimeException("Database offline");
        doThrow(databaseException).when(dltJpaRepository).saveAndFlush(dltRecord);

        RuntimeException thrown = assertThrows(RuntimeException.class, () -> adapter.persistDltRecord(dltRecord));

        assertEquals("Database offline", thrown.getMessage());
    }

    @SuppressWarnings("unchecked")
    private WeatherMap createMockWeatherMap(Set<String> geohashes) {
        WeatherMap weatherMap = mock(WeatherMap.class);
        Map<String, GridCellLayers> mockGridCellsMap = mock(Map.class);
        when(weatherMap.getGridCellsMap()).thenReturn(mockGridCellsMap);
        when(mockGridCellsMap.keySet()).thenReturn(geohashes);
        return weatherMap;
    }

    @SuppressWarnings("SameParameterValue")
    private WeatherMap createMockWeatherMap(Set<String> geohashes, int interval) {
        WeatherMap weatherMap = createMockWeatherMap(geohashes);
        when(weatherMap.getTimestampBucket()).thenReturn(TEST_TIMESTAMP);
        when(weatherMap.getIntervalMinutes()).thenReturn(interval);
        return weatherMap;
    }

    private void setupCacheManagers() {
        when(springL1CacheManager.getCache(BUCKETS_REGION)).thenReturn(bucketsCache);
        when(springL1CacheManager.getCache(QUERIES_REGION)).thenReturn(queriesCache);
    }

    private WeatherMapBucket createBaseBucket(int version) {
        WeatherMapBucket bucket = new WeatherMapBucket();
        bucket.setId(EXPECTED_BUCKET_ID);
        bucket.setTimestampBucket(TEST_TIMESTAMP);
        bucket.setIntervalMinutes(TEST_INTERVAL);
        bucket.setVersion(version);
        return bucket;
    }
}

