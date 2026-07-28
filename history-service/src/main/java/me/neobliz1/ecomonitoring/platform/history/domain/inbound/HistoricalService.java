package me.neobliz1.ecomonitoring.platform.history.domain.inbound;

import me.neobliz1.ecomonitoring.platform.history.domain.model.entity.WeatherMapBucket;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.WeatherMap;

public interface HistoricalService {

    void extractTelemetryFromWeatherMap(WeatherMap weatherMap, WeatherMapBucket bucket);
}
