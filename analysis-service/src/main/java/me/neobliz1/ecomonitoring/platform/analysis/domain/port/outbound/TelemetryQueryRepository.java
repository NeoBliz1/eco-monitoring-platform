package me.neobliz1.ecomonitoring.platform.analysis.domain.port.outbound;

import java.util.Map;

public interface TelemetryQueryRepository {

    Map<String, byte[]> findFilteredGridDataBySpatialBox(
            long activeBucketFloor,
            double minLat,
            double maxLat,
            double minLon,
            double maxLon
    );
}
