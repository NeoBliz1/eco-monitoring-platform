package me.neobliz1.ecomonitoring.platform.analysis.domain.port.outbound;

import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.WeatherMap;

public interface TelemetryQueryRepository {

    WeatherMap getWeatherMapByTimestampAndSpatialBox(
            long targetTimestamp,
            Double minLat,
            Double maxLat,
            Double minLon,
            Double maxLon
    );
}
