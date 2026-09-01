package me.neobliz1.ecomonitoring.platform.history.domain.port.outbound;

import me.neobliz1.ecomonitoring.platform.history.domain.model.entity.WeatherMapBucket;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.WeatherMap;

import java.util.UUID;

public interface HistoricalPersistenceRepository {

    WeatherMapBucket upsertBucket(UUID id, Long timestampBucket, Integer intervalMinutes);

    void persistTelemetryRecord(WeatherMap weatherMap);
}