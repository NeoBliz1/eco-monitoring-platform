package me.neobliz1.ecomonitoring.platform.analysis.domain.port.outbound;

import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.WeatherMap;

public interface TelemetryQueryArchive {

    WeatherMap findFilteredGridDataBySpatialBoxInArchive(
            long activeBucketFloor,
            double minLat,
            double maxLat,
            double minLon,
            double maxLon
    );
}