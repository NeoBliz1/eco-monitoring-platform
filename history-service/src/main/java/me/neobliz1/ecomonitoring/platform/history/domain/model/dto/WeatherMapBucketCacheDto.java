package me.neobliz1.ecomonitoring.platform.history.domain.model.dto;

import java.util.UUID;

public record WeatherMapBucketCacheDto(
        UUID id,
        long timestampBucket,
        int intervalMinutes,
        long version
) {
}