package me.neobliz1.ecomonitoring.platform.history.domain.port.inbound;

import me.neobliz1.ecomonitoring.platform.history.domain.model.entity.WeatherGridCellMetric;
import me.neobliz1.ecomonitoring.platform.history.domain.model.entity.WeatherMapBucket;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.GridCellLayers;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.WeatherMap;
import org.jspecify.annotations.NonNull;

import java.util.List;

public interface HistoricalDataConvertService {

    void mergeTelemetryInBatch(@NonNull WeatherMap weatherMap, @NonNull WeatherMapBucket bucket, @NonNull List<WeatherGridCellMetric> targetedCells);

    GridCellLayers convertWeatherGridCellsToWeatherMap(@NonNull WeatherGridCellMetric gridCellMetric);
}