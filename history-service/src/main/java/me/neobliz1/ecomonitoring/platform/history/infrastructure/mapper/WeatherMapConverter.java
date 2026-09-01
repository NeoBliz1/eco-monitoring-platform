package me.neobliz1.ecomonitoring.platform.history.infrastructure.mapper;

import lombok.NonNull;
import me.neobliz1.ecomonitoring.platform.history.domain.model.entity.WeatherGridCellMetric;
import me.neobliz1.ecomonitoring.platform.history.domain.model.entity.WeatherMapBucket;
import me.neobliz1.ecomonitoring.platform.history.domain.port.inbound.HistoricalDataConvertService;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.GridCellLayers;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.WeatherMap;

import java.util.Map;

public class WeatherMapConverter implements HistoricalDataConvertService {

    @Override
    public void extractTelemetryFromWeatherMap(@NonNull WeatherMap weatherMap, @NonNull WeatherMapBucket bucket) {
        for(Map.Entry<String, GridCellLayers> entry : weatherMap.getGridCellsMap().entrySet()) {
            String geohashKey = entry.getKey();
            GridCellLayers layers = entry.getValue();

            if(layers==null) {
                continue;
            }

            WeatherGridCellMetric cellMetric = new WeatherGridCellMetric();
            cellMetric.setBucketId(bucket.getId());
            cellMetric.setGeohash(geohashKey);
            cellMetric.setReadingCount(layers.getReadingCount());

            cellMetric.setAvgTemperature(layers.getAvgTemperature());
            cellMetric.setAvgHumidity(layers.getAvgHumidity());
            cellMetric.setAvgPressure(layers.getAvgPressure());
            cellMetric.setAvgLeaf_wetnessPct(layers.getAvgLeafWetnessPct());

            cellMetric.setAvgWindSpeed(layers.getAvgWindSpeed());
            cellMetric.setAvgWindDirection(layers.getAvgWindDirection());

            cellMetric.setAvgPm25(layers.getAvgPm25());
            cellMetric.setAvgPm10(layers.getAvgPm10());
            cellMetric.setAvgPm100(layers.getAvgPm100());

            cellMetric.setAvgVoc(layers.getAvgVoc());
            cellMetric.setAvgNoiseDb(layers.getAvgNoiseDb());

            cellMetric.setAvgRainMm(layers.getAvgRainMm());
            cellMetric.setAvgSnowCm(layers.getAvgSnowCm());
            cellMetric.setAvgEvapRate(layers.getAvgEvapRate());

            cellMetric.setAvgUvIndex(layers.getAvgUvIndex());
            cellMetric.setAvgSolarRadiationWm2(layers.getAvgSolarRadiationWm2());
            cellMetric.setAvgLux(layers.getAvgLux());
            cellMetric.setAvgVisibilityM(layers.getAvgVisibilityM());

            bucket.addCellMetric(cellMetric);
        }
    }

    @Override
    public @NonNull GridCellLayers convertWeatherGridCellsToWeatherMap(@NonNull WeatherGridCellMetric gridCellMetric) {
        return GridCellLayers.newBuilder()
                .setReadingCount(gridCellMetric.getReadingCount())

                .setAvgTemperature(gridCellMetric.getAvgTemperature())
                .setAvgHumidity(gridCellMetric.getAvgHumidity())
                .setAvgPressure(gridCellMetric.getAvgPressure())
                .setAvgLeafWetnessPct(gridCellMetric.getAvgLeaf_wetnessPct())

                .setAvgWindSpeed(gridCellMetric.getAvgWindSpeed())
                .setAvgWindDirection(gridCellMetric.getAvgWindDirection())

                .setAvgPm25(gridCellMetric.getAvgPm25())
                .setAvgPm10(gridCellMetric.getAvgPm10())
                .setAvgPm100(gridCellMetric.getAvgPm100())

                .setAvgVoc(gridCellMetric.getAvgVoc())
                .setAvgNoiseDb(gridCellMetric.getAvgNoiseDb())

                .setAvgRainMm(gridCellMetric.getAvgRainMm())
                .setAvgSnowCm(gridCellMetric.getAvgSnowCm())
                .setAvgEvapRate(gridCellMetric.getAvgEvapRate())

                .setAvgUvIndex(gridCellMetric.getAvgUvIndex())
                .setAvgSolarRadiationWm2(gridCellMetric.getAvgSolarRadiationWm2())
                .setAvgLux(gridCellMetric.getAvgLux())
                .setAvgVisibilityM(gridCellMetric.getAvgVisibilityM())
                .build();
    }
}
