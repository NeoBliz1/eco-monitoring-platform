package me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter.outbound.persistence.postgres;

import static me.neobliz1.ecomonitoring.platform.history.domain.model.constant.HistoricalCacheConstants.QUERIES_GLOBAL_REGION;
import static me.neobliz1.ecomonitoring.platform.history.domain.port.service.HistoricalUtils.getBucketIdFromWeatherMap;
import static me.neobliz1.ecomonitoring.platform.history.domain.port.service.HistoricalUtils.getL1BucketCache;
import static org.springframework.transaction.annotation.Propagation.REQUIRES_NEW;

import io.github.neobliz1.validproto.annotation.ValidProto;
import io.github.neobliz1.validproto.annotation.ValidatedProto;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import me.neobliz1.ecomonitoring.platform.history.domain.model.dto.WeatherMapBucketCacheDto;
import me.neobliz1.ecomonitoring.platform.history.domain.model.entity.WeatherMapBucket;
import me.neobliz1.ecomonitoring.platform.history.domain.model.entity.WeatherTelemetryDlqRecord;
import me.neobliz1.ecomonitoring.platform.history.domain.port.inbound.HistoricalDataConvertService;
import me.neobliz1.ecomonitoring.platform.history.domain.port.outbound.HistoricalPersistenceRepository;
import me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter.outbound.persistence.postgres.jpa.HistoricalWeatherMapJpaRepository;
import me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter.outbound.persistence.postgres.jpa.HistoricalWeatherTelemetryDltJpaRepository;
import me.neobliz1.ecomonitoring.platform.model.exception.L1CacheNotAvailableException;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.WeatherMap;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;
import java.util.UUID;

@Slf4j
@ValidatedProto
@RequiredArgsConstructor
public class HistoricalPersistenceRepositoryAdapter implements HistoricalPersistenceRepository {

    private final WeatherMapBucketPersistenceAdapter weatherMapBucketPersistenceAdapter;
    private final HistoricalWeatherTelemetryDltJpaRepository dltJpaRepository;
    private final HistoricalDataConvertService weatherMapConverter;
    private final HistoricalWeatherMapJpaRepository jpaRepository;
    @Nullable
    private final HistoricalTxIdRepositoryAdapter txIdAdapter;
    private final CacheManager springL1CacheManager;

    @Override
    @Transactional(isolation = Isolation.READ_COMMITTED)
    public void persistTelemetryRecord(@NonNull @ValidProto WeatherMap weatherMap) {
        checkIfAtLeastOneGridCellLayerExists(weatherMap);
        WeatherMapBucket weatherMapBucket = getWeatherMapBucket(weatherMap).orElseGet(() -> persistWeatherMapBucket(weatherMap));
        UUID bucketId = getBucketIdFromWeatherMap(weatherMap);
        WeatherMapBucketCacheDto cachedDto = getL1BucketCache(springL1CacheManager).get(bucketId, WeatherMapBucketCacheDto.class);
        if(cachedDto==null) {
            putSavedBucketToCacheByUuid(weatherMapBucket);
        }
        weatherMapConverter.mergeTelemetryInBatch(weatherMap, weatherMapBucket);
        getL1QueryCache().clear();
        persistTelemetryTxId(weatherMap);
        if(log.isDebugEnabled()) {
            log.debug("Successfully persisted WeatherMap snapshot bucket: {}", weatherMap.getTimestampBucket());
        }
    }

    public void checkIfAtLeastOneGridCellLayerExists(@NonNull WeatherMap weatherMap) {
        if(weatherMap.getGridCellsMap().isEmpty()) {
            throw new IllegalArgumentException("Invalid telemetry packet: WeatherMap must contain at least one grid cell metric.");
        }
    }

    private Optional<WeatherMapBucket> getWeatherMapBucket(@NonNull WeatherMap weatherMap) {
        UUID bucketId = getBucketIdFromWeatherMap(weatherMap);
        Optional<WeatherMapBucket> optionalBucket;
        WeatherMapBucketCacheDto cachedDto = getL1BucketCache(springL1CacheManager).get(bucketId, WeatherMapBucketCacheDto.class);
        if(cachedDto==null) {
            optionalBucket = jpaRepository.findById(bucketId);
        } else {
            optionalBucket = Optional.of(jpaRepository.getReferenceById(bucketId));
        }
        return optionalBucket;
    }

    private WeatherMapBucket persistWeatherMapBucket(@NonNull WeatherMap weatherMap) {
        UUID bucketId = getBucketIdFromWeatherMap(weatherMap);
        try {
            return weatherMapBucketPersistenceAdapter.saveWeatherMapBucket(weatherMap);
        } catch(DataIntegrityViolationException e) {
            if(log.isDebugEnabled()) {
                log.debug("Collision hit during bucket creation for ID {}. Recovering from winner thread.", bucketId);
            }
            return jpaRepository.findById(bucketId)
                    .orElseThrow(() -> new IllegalStateException("Bucket vanished after concurrent collision constraint", e));
        }
    }

    private void putSavedBucketToCacheByUuid(@NonNull WeatherMapBucket savedBucket) {
        UUID bucketUuid = savedBucket.getId();
        WeatherMapBucketCacheDto dtoToCache = new WeatherMapBucketCacheDto(
                bucketUuid,
                savedBucket.getTimestampBucket(),
                savedBucket.getIntervalMinutes(),
                savedBucket.getVersion()
        );
        getL1BucketCache(springL1CacheManager).put(bucketUuid, dtoToCache);
    }

    private @NonNull Cache getL1QueryCache() {
        Cache grpcQueryCache = springL1CacheManager.getCache(QUERIES_GLOBAL_REGION);
        if(grpcQueryCache==null) {
            throw new L1CacheNotAvailableException(QUERIES_GLOBAL_REGION);
        }
        return grpcQueryCache;
    }

    private void persistTelemetryTxId(@NonNull WeatherMap weatherMap) {
        if(txIdAdapter!=null) {
            if(log.isDebugEnabled()) {
                log.debug("Confirmation profile active. Executing transaction batch log tracking.");
            }
            txIdAdapter.processTxIdsHistoryBatch(weatherMap);
        }
    }

    @Override
    @Transactional(propagation = REQUIRES_NEW, isolation = Isolation.READ_COMMITTED)
    public void persistDltRecord(@NonNull WeatherTelemetryDlqRecord dltRecord) {
        try {
            dltJpaRepository.saveAndFlush(dltRecord);
            if(log.isDebugEnabled()) {
                log.debug("Successfully archived dead record to PostgreSQL DLT table. ID: {}, Original Offset: {}",
                        dltRecord.getId(), dltRecord.getOriginalOffset());
            }
        } catch(Exception e) {
            log.error("Failed to write dead letter archive to database for topic {} partition {} offset {}",
                    dltRecord.getOriginalTopic(), dltRecord.getPartitionId(), dltRecord.getOriginalOffset(), e);
            throw e;
        }
    }
}
