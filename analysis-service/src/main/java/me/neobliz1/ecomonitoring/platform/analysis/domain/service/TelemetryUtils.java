package me.neobliz1.ecomonitoring.platform.analysis.domain.service;

import static me.neobliz1.ecomonitoring.platform.analysis.domain.model.AnalysisConstants.GRID_BUCKET_KEY_FORMAT;

import lombok.experimental.UtilityClass;
import me.neobliz1.ecomonitoring.platform.analysis.domain.model.AnalysisConstants;
import org.jspecify.annotations.NonNull;

import java.time.Duration;

@UtilityClass
public class TelemetryUtils {

    private static final double MAX_LATITUDE = 85.051;
    private static final double MIN_LATITUDE = -85.051;
    private static final double MAX_LONGITUDE = 180.0;
    private static final double MIN_LONGITUDE = -180.0;

    public static long getAggregationBucketFloorInterval(long packetTimestampInMillis, int aggregationSecondsPerInterval) {
        if(packetTimestampInMillis<0 || aggregationSecondsPerInterval<0) {
            return 0L;
        }
        long aggIntervalMillis = TelemetryUtils.getMillis(aggregationSecondsPerInterval);
        return packetTimestampInMillis/aggIntervalMillis*aggIntervalMillis;
    }

    public static long getMillis(long intervalSeconds) {
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
        return Math.round(c*AnalysisConstants.SCALE_COFF)/AnalysisConstants.SCALE_COFF;
    }

    public static @NonNull String getSpatialIndexKey(long activeBucketFloor) {
        return "spatial_index:"+String.format(GRID_BUCKET_KEY_FORMAT, activeBucketFloor);
    }
}