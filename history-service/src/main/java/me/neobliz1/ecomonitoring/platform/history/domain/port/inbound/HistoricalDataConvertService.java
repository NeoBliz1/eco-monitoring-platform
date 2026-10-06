package me.neobliz1.ecomonitoring.platform.history.domain.port.inbound;

import me.neobliz1.ecomonitoring.platform.history.domain.model.entity.WeatherGridCellLayer;
import me.neobliz1.ecomonitoring.platform.history.domain.model.entity.WeatherMapBucket;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.GridCellLayers;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.WeatherMap;
import org.jspecify.annotations.NonNull;

public interface HistoricalDataConvertService {

    void mergeTelemetryInBatch(@NonNull WeatherMap weatherMap, @NonNull WeatherMapBucket bucket);

    GridCellLayers convertWeatherGridCellsToWeatherMap(@NonNull WeatherGridCellLayer gridCellMetric);
}