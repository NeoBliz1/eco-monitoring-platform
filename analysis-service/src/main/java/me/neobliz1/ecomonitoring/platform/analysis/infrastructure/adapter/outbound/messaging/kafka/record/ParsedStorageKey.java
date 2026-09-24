package me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.messaging.kafka.record;

import static me.neobliz1.ecomonitoring.platform.common.constant.PlatformConstants.GEOHASH_SEPARATOR;

import org.jspecify.annotations.NonNull;

public record ParsedStorageKey(
        @NonNull String bucketTime,
        @NonNull String geohash,
        String txId,
        String spatialKey,
        String lat,
        String lon
) {

    public ParsedStorageKey(@NonNull String bucketTime, @NonNull String geohash, String txId) {
        this(bucketTime, geohash, txId, null, null, null);
    }

    public ParsedStorageKey(@NonNull String bucketTime, @NonNull String geohash, String txId,
                            String spatialKey, String lat, String lon) {
        String computedSpatialKey = bucketTime+GEOHASH_SEPARATOR+geohash;
        int sep = geohash.indexOf(GEOHASH_SEPARATOR);
        if(sep==-1) {
            throw new IndexOutOfBoundsException("Geohash coordinates format is invalid: "+geohash);
        }
        String computedLat = geohash.substring(0, sep);
        String computedLon = geohash.substring(sep+1);
        validateComputedValues(spatialKey, lat, lon, computedSpatialKey, computedLat, computedLon);
        this.bucketTime = bucketTime;
        this.geohash = geohash;
        this.txId = txId;
        this.spatialKey = computedSpatialKey;
        this.lat = computedLat;
        this.lon = computedLon;
    }

    private static void validateComputedValues(String spatialKey, String lat, String lon, String computedSpatialKey, String computedLat, String computedLon) {
        if(spatialKey!=null && !spatialKey.equals(computedSpatialKey)) {
            throw new IllegalArgumentException("Provided spatialKey does not match computed value");
        }
        if(lat!=null && !lat.equals(computedLat)) {
            throw new IllegalArgumentException("Provided lat does not match computed value");
        }
        if(lon!=null && !lon.equals(computedLon)) {
            throw new IllegalArgumentException("Provided lon does not match computed value");
        }
    }

    public static @NonNull ParsedStorageKey parseSpatialKey(@NonNull String spatialKey) {
        int firstSep = spatialKey.indexOf(GEOHASH_SEPARATOR);
        if(firstSep==-1) {
            throw new IndexOutOfBoundsException("Spatial key format is invalid: "+spatialKey);
        }
        String bucketTime = spatialKey.substring(0, firstSep);
        String geohash = spatialKey.substring(firstSep+1);
        return new ParsedStorageKey(bucketTime, geohash, null);
    }

    public static @NonNull ParsedStorageKey parseAggKey(@NonNull String aggKey) {
        int firstSep = aggKey.indexOf(GEOHASH_SEPARATOR);
        int lastSep = aggKey.lastIndexOf(GEOHASH_SEPARATOR);
        if(firstSep==-1 || lastSep==-1 || firstSep==lastSep) {
            throw new IndexOutOfBoundsException("Key format has been invalid: "+aggKey);
        }
        String bucketTime = aggKey.substring(0, firstSep);
        String geohash = aggKey.substring(firstSep+1, lastSep);
        String txId = aggKey.substring(lastSep+1);
        return new ParsedStorageKey(bucketTime, geohash, txId);
    }
}