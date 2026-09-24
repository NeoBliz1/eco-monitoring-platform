package me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter.inbound.grpc.record;

public record SpatialQueryKey(long timestampBucket, int timeIntervalInMinutes,
                              double minLat, double maxLat, double minLon, double maxLon) {
}