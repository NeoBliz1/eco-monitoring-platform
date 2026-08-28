package me.neobliz1.ecomonitoring.platform.analysis.domain.service;

import lombok.experimental.UtilityClass;

import java.time.Duration;

@UtilityClass
public class TelemetryUtils {

    private static final double MAX_LATITUDE = 85.05112878;
    private static final double MIN_LATITUDE = -85.05112878;
    private static final double MAX_LONGITUDE = 180.0;
    private static final double MIN_LONGITUDE = -180.0;

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

    public static double clampLatitude(double lat) {
        return Math.max(MIN_LATITUDE, Math.min(MAX_LATITUDE, roundCoordinate(lat)));
    }

    public static double clampLongitude(double lon) {
        double roundedLon = roundCoordinate(lon);
        if(roundedLon>MAX_LONGITUDE || roundedLon<MIN_LONGITUDE) {
            return ((roundedLon+180.0)%360.0+360.0)%360.0-180.0;
        }
        return roundedLon;
    }

    private static double roundCoordinate(double c) {
        return Math.round(c*10.0)/10.0;
    }
}