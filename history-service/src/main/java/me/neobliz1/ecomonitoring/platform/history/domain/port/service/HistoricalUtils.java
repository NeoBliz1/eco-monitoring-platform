package me.neobliz1.ecomonitoring.platform.history.domain.port.service;

import static me.neobliz1.ecomonitoring.platform.history.domain.model.constant.HistoricalCacheConstants.BUCKETS_GLOBAL_REGION;

import me.neobliz1.ecomonitoring.platform.model.exception.L1CacheNotAvailableException;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.WeatherMap;
import org.jspecify.annotations.NonNull;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import weather.history.SpatialBoxRequest;

import java.util.UUID;

public class HistoricalUtils {

    public static @NonNull UUID getBucketIdFromWeatherMap(@NonNull WeatherMap weatherMap) {
        String timestamp = String.valueOf(weatherMap.getTimestampBucket());
        int intervalMinutes = weatherMap.getIntervalMinutes();
        return getUuidBasedOnStringBytes(timestamp+intervalMinutes);
    }

    public static @NonNull UUID getBucketIdFromSpatialBoxRequest(@NonNull SpatialBoxRequest request) {
        String timestamp = String.valueOf(request.getTimestampBucket());
        int intervalInMinutes = request.getTimeIntervalInMinutes();
        return getUuidBasedOnStringBytes(timestamp+intervalInMinutes);
    }

    private static @NonNull UUID getUuidBasedOnStringBytes(String uuidBaseString) {
        return UUID.nameUUIDFromBytes((uuidBaseString).getBytes());
    }

    public static @NonNull Cache getL1BucketCache(CacheManager springL1CacheManager) {
        Cache springCache = springL1CacheManager.getCache(BUCKETS_GLOBAL_REGION);
        if(springCache==null) {
            throw new L1CacheNotAvailableException(BUCKETS_GLOBAL_REGION);
        }
        return springCache;
    }
}