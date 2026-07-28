package me.neobliz1.ecomonitoring.platform.analysis.domain.port.inbound;


import java.util.List;

public interface TelemetryQueryService {

    String getLatestTimeIntervalWeatherMapByCoordinates(long targetTimestamp, List<Double> coordinatesSquare);
}
