package me.neobliz1.ecomonitoring.platform.history.infrastructure.mapper;

import lombok.RequiredArgsConstructor;
import me.neobliz1.ecomonitoring.platform.history.domain.model.entity.WeatherGridCellMetric;
import me.neobliz1.ecomonitoring.platform.history.domain.model.entity.WeatherMapBucket;
import me.neobliz1.ecomonitoring.platform.history.domain.port.inbound.HistoricalDataConvertService;
import me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter.outbound.persistence.postgres.jpa.HistoricalWeatherGridCellJpaRepository;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.GridCellLayers;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.WeatherMap;
import org.jspecify.annotations.NonNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RequiredArgsConstructor
public class WeatherMapConverter implements HistoricalDataConvertService {

    private final HistoricalWeatherGridCellJpaRepository gridCellJpaRepository;

    private static @NonNull WeatherGridCellMetric getWeatherGridCellMetric(@NonNull WeatherMapBucket bucket,
                                                                           @NonNull String geohashKey,
                                                                           @NonNull GridCellLayers layers) {
        WeatherGridCellMetric newCellMetric = new WeatherGridCellMetric(bucket, geohashKey);

        newCellMetric.setBucketId(bucket.getId());
        newCellMetric.setGeohash(geohashKey);
        newCellMetric.setReadingCount(layers.getReadingCount());

        newCellMetric.setAvgTemperature(layers.getAvgTemperature());
        newCellMetric.setAvgHumidity(layers.getAvgHumidity());
        newCellMetric.setAvgPressure(layers.getAvgPressure());
        newCellMetric.setAvgLeaf_wetnessPct(layers.getAvgLeafWetnessPct());

        newCellMetric.setAvgWindSpeed(layers.getAvgWindSpeed());
        newCellMetric.setAvgWindDirection(layers.getAvgWindDirection());

        newCellMetric.setAvgPm25(layers.getAvgPm25());
        newCellMetric.setAvgPm10(layers.getAvgPm10());
        newCellMetric.setAvgPm100(layers.getAvgPm100());

        newCellMetric.setAvgVoc(layers.getAvgVoc());
        newCellMetric.setAvgNoiseDb(layers.getAvgNoiseDb());

        newCellMetric.setAvgRainMm(layers.getAvgRainMm());
        newCellMetric.setAvgSnowCm(layers.getAvgSnowCm());
        newCellMetric.setAvgEvapRate(layers.getAvgEvapRate());

        newCellMetric.setAvgUvIndex(layers.getAvgUvIndex());
        newCellMetric.setAvgSolarRadiationWm2(layers.getAvgSolarRadiationWm2());
        newCellMetric.setAvgLux(layers.getAvgLux());
        newCellMetric.setAvgVisibilityM(layers.getAvgVisibilityM());
        return newCellMetric;
    }

    @Override
    public void mergeTelemetryInBatch(
            @NonNull WeatherMap weatherMap,
            @NonNull WeatherMapBucket bucket,
            @NonNull List<WeatherGridCellMetric> targetedCells) {

        if(weatherMap.getGridCellsMap().isEmpty()) {
            return;
        }

        Map<String, WeatherGridCellMetric> existingCellsMap = targetedCells.stream()
                .collect(Collectors.toMap(
                        WeatherGridCellMetric::getGeohash,
                        cell -> cell,
                        (existing, replacement) -> existing
                ));

        List<WeatherGridCellMetric> cellsToSave = new ArrayList<>();

        for(Map.Entry<String, GridCellLayers> entry : weatherMap.getGridCellsMap().entrySet()) {
            String geohashKey = entry.getKey();
            GridCellLayers layers = entry.getValue();
            if(layers==null) continue;

            WeatherGridCellMetric targetCell = existingCellsMap.get(geohashKey);

            if(targetCell!=null) {
                targetCell.setReadingCount(targetCell.getReadingCount()+layers.getReadingCount());

                targetCell.setAvgTemperature(mergeAverage(targetCell.getAvgTemperature(), layers.getAvgTemperature()));
                targetCell.setAvgHumidity(mergeAverage(targetCell.getAvgHumidity(), layers.getAvgHumidity()));
                targetCell.setAvgPressure(mergeAverage(targetCell.getAvgPressure(), layers.getAvgPressure()));
                targetCell.setAvgLeaf_wetnessPct(mergeAverage(targetCell.getAvgLeaf_wetnessPct(), layers.getAvgLeafWetnessPct()));

                targetCell.setAvgWindSpeed(mergeAverage(targetCell.getAvgWindSpeed(), layers.getAvgWindSpeed()));
                targetCell.setAvgWindDirection(mergeAverage(targetCell.getAvgWindDirection(), layers.getAvgWindDirection()));

                targetCell.setAvgPm25(mergeAverage(targetCell.getAvgPm25(), layers.getAvgPm25()));
                targetCell.setAvgPm10(mergeAverage(targetCell.getAvgPm10(), layers.getAvgPm10()));
                targetCell.setAvgPm100(mergeAverage(targetCell.getAvgPm100(), layers.getAvgPm100()));

                targetCell.setAvgVoc(mergeAverage(targetCell.getAvgVoc(), layers.getAvgVoc()));
                targetCell.setAvgNoiseDb(mergeAverage(targetCell.getAvgNoiseDb(), layers.getAvgNoiseDb()));

                targetCell.setAvgRainMm(mergeAverage(targetCell.getAvgRainMm(), layers.getAvgRainMm()));
                targetCell.setAvgSnowCm(mergeAverage(targetCell.getAvgSnowCm(), layers.getAvgSnowCm()));
                targetCell.setAvgEvapRate(mergeAverage(targetCell.getAvgEvapRate(), layers.getAvgEvapRate()));

                targetCell.setAvgUvIndex(mergeAverage(targetCell.getAvgUvIndex(), layers.getAvgUvIndex()));
                targetCell.setAvgSolarRadiationWm2(mergeAverage(targetCell.getAvgSolarRadiationWm2(), layers.getAvgSolarRadiationWm2()));
                targetCell.setAvgLux(mergeAverage(targetCell.getAvgLux(), layers.getAvgLux()));
                targetCell.setAvgVisibilityM(mergeAverage(targetCell.getAvgVisibilityM(), layers.getAvgVisibilityM()));
            } else {
                WeatherGridCellMetric newCellMetric = getWeatherGridCellMetric(bucket, geohashKey, layers);
                cellsToSave.add(newCellMetric);
            }
        }

        if(!cellsToSave.isEmpty()) {
            gridCellJpaRepository.saveAll(cellsToSave);
        }
    }

    private Double mergeAverage(Double oldVal, Double newVal) {
        if(oldVal==null) return newVal;
        if(newVal==null) return oldVal;
        return (oldVal+newVal)/2.0;
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
