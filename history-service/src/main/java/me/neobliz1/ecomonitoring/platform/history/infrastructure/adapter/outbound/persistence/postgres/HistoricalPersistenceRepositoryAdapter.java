package me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter.outbound.persistence.postgres;

import static me.neobliz1.ecomonitoring.platform.history.domain.model.constant.HistoricalCacheConstants.BUCKETS_REGION;
import static me.neobliz1.ecomonitoring.platform.history.domain.model.constant.HistoricalCacheConstants.QUERIES_REGION;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import me.neobliz1.ecomonitoring.platform.history.domain.model.dto.WeatherMapBucketCacheDto;
import me.neobliz1.ecomonitoring.platform.history.domain.model.entity.WeatherGridCellMetric;
import me.neobliz1.ecomonitoring.platform.history.domain.model.entity.WeatherMapBucket;
import me.neobliz1.ecomonitoring.platform.history.domain.model.entity.WeatherTelemetryDltRecord;
import me.neobliz1.ecomonitoring.platform.history.domain.port.inbound.HistoricalDataConvertService;
import me.neobliz1.ecomonitoring.platform.history.domain.port.outbound.HistoricalPersistenceRepository;
import me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter.outbound.persistence.postgres.jpa.HistoricalWeatherGridCellJpaRepository;
import me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter.outbound.persistence.postgres.jpa.HistoricalWeatherMapJpaRepository;
import me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter.outbound.persistence.postgres.jpa.HistoricalWeatherTelemetryDltJpaRepository;
import me.neobliz1.ecomonitoring.platform.model.exception.L1CacheNotAvailableException;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.WeatherMap;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Slf4j
@RequiredArgsConstructor
public class HistoricalPersistenceRepositoryAdapter implements HistoricalPersistenceRepository {

    private final HistoricalWeatherTelemetryDltJpaRepository dltJpaRepository;
    private final HistoricalWeatherGridCellJpaRepository gridCellJpaRepository;
    private final HistoricalDataConvertService weatherMapConverter;
    private final HistoricalWeatherMapJpaRepository jpaRepository;
    @Nullable
    private final HistoricalTxIdRepositoryAdapter txIdAdapter;
    private final CacheManager springL1CacheManager;

    public static @NonNull UUID getBucketId(@NonNull WeatherMap weatherMap) {
        return UUID.nameUUIDFromBytes((String.valueOf(weatherMap.getTimestampBucket())+weatherMap.getIntervalMinutes()).getBytes());
    }

    @Override
    @Transactional(isolation = Isolation.READ_COMMITTED)
    public void persistTelemetryRecord(@NonNull WeatherMap weatherMap) {
        Cache springCache = springL1CacheManager.getCache(BUCKETS_REGION);
        Cache grpcQueryCache = springL1CacheManager.getCache(QUERIES_REGION);
        if(springCache==null || grpcQueryCache==null) {
            throw new L1CacheNotAvailableException(springCache==null?BUCKETS_REGION:QUERIES_REGION);
        }
        weatherMap.getGridCellsMap();
        if(weatherMap.getGridCellsMap().isEmpty()) {
            throw new IllegalArgumentException("Invalid telemetry packet: WeatherMap must contain at least one grid cell metric.");
        }
        UUID bucketId = getBucketId(weatherMap);
        WeatherMapBucket weatherMapBucket;
        boolean isBrandNewInsert = false;
        WeatherMapBucketCacheDto cachedDto = springCache.get(bucketId, WeatherMapBucketCacheDto.class);
        List<WeatherGridCellMetric> targetedExistingCells = List.of();
        if(cachedDto==null) {
            Optional<WeatherMapBucket> optionalBucket = jpaRepository.findById(bucketId);
            if(optionalBucket.isPresent()) {
                weatherMapBucket = optionalBucket.get();
                targetedExistingCells = gridCellJpaRepository.findSpecificCellsForMerge(
                        bucketId, weatherMap.getGridCellsMap().keySet()
                );
            } else {
                isBrandNewInsert = true;
                weatherMapBucket = new WeatherMapBucket();
                weatherMapBucket.setId(bucketId);
                weatherMapBucket.setTimestampBucket(weatherMap.getTimestampBucket());
                weatherMapBucket.setIntervalMinutes(weatherMap.getIntervalMinutes());
                weatherMapBucket = jpaRepository.saveAndFlush(weatherMapBucket);
            }
        } else {
            weatherMapBucket = jpaRepository.getReferenceById(bucketId);
            targetedExistingCells = gridCellJpaRepository.findSpecificCellsForMerge(
                    bucketId, weatherMap.getGridCellsMap().keySet()
            );
        }
        weatherMapConverter.mergeTelemetryInBatch(weatherMap, weatherMapBucket, targetedExistingCells);
        WeatherMapBucket savedBucket = jpaRepository.saveAndFlush(weatherMapBucket);
        WeatherMapBucketCacheDto dtoToCache = new WeatherMapBucketCacheDto(
                savedBucket.getId(),
                savedBucket.getTimestampBucket(),
                savedBucket.getIntervalMinutes(),
                savedBucket.getVersion()
        );
        if(!isBrandNewInsert) {
            grpcQueryCache.clear();
        }
        springCache.put(bucketId, dtoToCache);
        if(txIdAdapter!=null) {
            if(log.isDebugEnabled()) {
                log.debug("Confirmation profile active. Executing transaction batch log tracking.");
            }
            txIdAdapter.processTxIdsHistoryBatch(weatherMap);
        }
        if(log.isDebugEnabled()) {
            log.debug("Successfully persisted WeatherMap snapshot bucket: {}", weatherMapBucket.getTimestampBucket());
        }
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, isolation = Isolation.READ_COMMITTED)
    public void persistDltRecord(@NonNull WeatherTelemetryDltRecord dltRecord) {
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
