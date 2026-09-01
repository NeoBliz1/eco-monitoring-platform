package me.neobliz1.ecomonitoring.platform.history.domain.port.inbound;

import me.neobliz1.ecomonitoring.platform.history.domain.model.entity.WeatherGridCellMetric;
import me.neobliz1.ecomonitoring.platform.history.domain.model.entity.WeatherMapBucket;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.GridCellLayers;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.WeatherMap;

public interface HistoricalDataConvertService {

    void extractTelemetryFromWeatherMap(WeatherMap weatherMap, WeatherMapBucket bucket);

    GridCellLayers convertWeatherGridCellsToWeatherMap(WeatherGridCellMetric gridCellMetric);
}