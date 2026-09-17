package me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter.inbound.grpc;

public record SpatialQueryKey(long timestampBucket, int timeIntervalInMinutes,
                              double minLat, double maxLat, double minLon, double maxLon) {
}