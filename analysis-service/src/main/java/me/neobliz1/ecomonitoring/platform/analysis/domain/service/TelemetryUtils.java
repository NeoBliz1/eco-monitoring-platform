package me.neobliz1.ecomonitoring.platform.analysis.domain.service;

import lombok.experimental.UtilityClass;

import java.time.Duration;

@UtilityClass
public class TelemetryUtils {

    public static long getAggregationBucketFloorInterval(long packetTimestampInMillis, int aggregationSecondsPerInterval) {
        if(packetTimestampInMillis<0 || aggregationSecondsPerInterval<0) {
            return 0L;
        }
        long aggIntervalMillis = TelemetryUtils.getMillis(aggregationSecondsPerInterval);
        return packetTimestampInMillis/aggIntervalMillis*aggIntervalMillis;
    }

    private static long getMillis(long intervalSeconds) {
        return Duration.ofSeconds(intervalSeconds).toMillis();
    }
}