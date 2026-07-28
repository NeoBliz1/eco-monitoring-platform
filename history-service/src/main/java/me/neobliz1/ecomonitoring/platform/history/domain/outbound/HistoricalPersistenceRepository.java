package me.neobliz1.ecomonitoring.platform.history.domain.outbound;

import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.WeatherMap;

public interface HistoricalPersistenceRepository {

    void persistTelemetryRecord(WeatherMap weatherMap);
}
