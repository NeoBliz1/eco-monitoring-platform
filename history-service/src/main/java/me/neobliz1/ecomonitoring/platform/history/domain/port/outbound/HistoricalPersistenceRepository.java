package me.neobliz1.ecomonitoring.platform.history.domain.port.outbound;

import me.neobliz1.ecomonitoring.platform.history.domain.model.entity.WeatherTelemetryDltRecord;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.WeatherMap;
import org.jspecify.annotations.NonNull;

public interface HistoricalPersistenceRepository {

    void persistTelemetryRecord(@NonNull WeatherMap weatherMap);

    void persistDltRecord(@NonNull WeatherTelemetryDltRecord dltRecord);
}