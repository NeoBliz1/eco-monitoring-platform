package me.neobliz1.ecomonitoring.platform.analysis.domain.port.inbound;


import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.WeatherMap;

public interface TelemetryQueryService {

    WeatherMap getLatestTimeIntervalWeatherMapByCoordinates(long targetTimestamp, Double minLat, Double maxLat, Double minLon, Double maxLon);
}
