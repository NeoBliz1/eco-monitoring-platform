package me.neobliz1.ecomonitoring.platform.history.infrastructure.mapper;

import static me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter.outbound.persistence.postgres.HistoricalPersistenceRepositoryAdapter.getBucketId;

import lombok.RequiredArgsConstructor;
import me.neobliz1.ecomonitoring.platform.history.domain.model.entity.WeatherGridCellLayer;
import me.neobliz1.ecomonitoring.platform.history.domain.model.entity.WeatherMapBucket;
import me.neobliz1.ecomonitoring.platform.history.domain.port.inbound.HistoricalDataConvertService;
import me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter.outbound.persistence.postgres.jpa.HistoricalWeatherGridCellJpaRepository;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.GridCellLayers;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.WeatherMap;
import org.jspecify.annotations.NonNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@RequiredArgsConstructor
public class WeatherMapConverter implements HistoricalDataConvertService {

    private final HistoricalWeatherGridCellJpaRepository gridCellJpaRepository;

    private static Double mergeWeightedAverage(Double oldVal, long oldCount, Double newVal, int newCount) {
        if(oldVal==null) return newVal;
        if(newVal==null) return oldVal;
        if(oldCount<=0) return newVal;
        if(newCount<=0) return oldVal;
        return ((oldVal*oldCount)+(newVal*newCount))/(double) (oldCount+newCount);
    }

    private static @NonNull WeatherGridCellLayer createWeatherGridCellLayer(@NonNull GridCellLayers gridCellsLayer,
                                                                            @NonNull WeatherMapBucket bucket,
                                                                            @NonNull String geohashKey) {
        WeatherGridCellLayer newCellMetric = new WeatherGridCellLayer(bucket, geohashKey);

        newCellMetric.setBucketId(bucket.getId());
        newCellMetric.setGeohash(geohashKey);
        newCellMetric.setReadingCount(gridCellsLayer.getReadingCount());

        newCellMetric.setAvgTemperature(gridCellsLayer.getAvgTemperature());
        newCellMetric.setAvgHumidity(gridCellsLayer.getAvgHumidity());
        newCellMetric.setAvgPressure(gridCellsLayer.getAvgPressure());
        newCellMetric.setAvgLeaf_wetnessPct(gridCellsLayer.getAvgLeafWetnessPct());

        newCellMetric.setAvgWindSpeed(gridCellsLayer.getAvgWindSpeed());
        newCellMetric.setAvgWindDirection(gridCellsLayer.getAvgWindDirection());

        newCellMetric.setAvgPm25(gridCellsLayer.getAvgPm25());
        newCellMetric.setAvgPm10(gridCellsLayer.getAvgPm10());
        newCellMetric.setAvgPm100(gridCellsLayer.getAvgPm100());

        newCellMetric.setAvgVoc(gridCellsLayer.getAvgVoc());
        newCellMetric.setAvgNoiseDb(gridCellsLayer.getAvgNoiseDb());

        newCellMetric.setAvgRainMm(gridCellsLayer.getAvgRainMm());
        newCellMetric.setAvgSnowCm(gridCellsLayer.getAvgSnowCm());
        newCellMetric.setAvgEvapRate(gridCellsLayer.getAvgEvapRate());

        newCellMetric.setAvgUvIndex(gridCellsLayer.getAvgUvIndex());
        newCellMetric.setAvgSolarRadiationWm2(gridCellsLayer.getAvgSolarRadiationWm2());
        newCellMetric.setAvgLux(gridCellsLayer.getAvgLux());
        newCellMetric.setAvgVisibilityM(gridCellsLayer.getAvgVisibilityM());
        return newCellMetric;
    }

    @Override
    public void mergeTelemetryInBatch(@NonNull WeatherMap weatherMap, @NonNull WeatherMapBucket bucket) {
        if(weatherMap.getGridCellsMap().isEmpty()) {
            return;
        }
        Map<String, WeatherGridCellLayer> existingCellsMap = convertExistsGridCellsSetToMap(weatherMap);
        List<WeatherGridCellLayer> cellsToSave = new ArrayList<>();
        for(Map.Entry<String, GridCellLayers> entry : weatherMap.getGridCellsMap().entrySet()) {
            String geohashKey = entry.getKey();
            GridCellLayers gridCellsLayer = entry.getValue();
            if(gridCellsLayer==null) continue;
            WeatherGridCellLayer targetCell = existingCellsMap.get(geohashKey);
            if(targetCell!=null) {
                mergeIntoExistingCell(targetCell, gridCellsLayer);
                cellsToSave.add(targetCell);
            } else {
                WeatherGridCellLayer newCellMetric = createWeatherGridCellLayer(gridCellsLayer, bucket, geohashKey);
                cellsToSave.add(newCellMetric);
            }
        }
        if(!cellsToSave.isEmpty()) {
            gridCellJpaRepository.saveAllAndFlush(cellsToSave);
        }
    }

    private @NonNull Map<String, WeatherGridCellLayer> convertExistsGridCellsSetToMap(@NonNull WeatherMap weatherMap) {
        List<WeatherGridCellLayer> existingGridCells = getExistingGridCells(weatherMap);
        Map<String, WeatherGridCellLayer> gridCellLayers = new HashMap<>();
        for(WeatherGridCellLayer layer : existingGridCells) {
            gridCellLayers.put(layer.getGeohash(), layer);
        }
        return gridCellLayers;
    }

    private @NonNull List<WeatherGridCellLayer> getExistingGridCells(@NonNull WeatherMap weatherMap) {
        Set<String> geohashes = weatherMap.getGridCellsMap().keySet();
        return gridCellJpaRepository.findSpecificGridCellLayersForMerge(getBucketId(weatherMap), geohashes);
    }

    private void mergeIntoExistingCell(WeatherGridCellLayer targetCell, GridCellLayers layers) {
        int oldCount = targetCell.getReadingCount();
        int newCount = layers.getReadingCount();
        int combinedCount = oldCount+newCount;

        targetCell.setReadingCount(combinedCount);

        targetCell.setAvgTemperature(mergeWeightedAverage(targetCell.getAvgTemperature(), oldCount, layers.getAvgTemperature(), newCount));
        targetCell.setAvgHumidity(mergeWeightedAverage(targetCell.getAvgHumidity(), oldCount, layers.getAvgHumidity(), newCount));
        targetCell.setAvgPressure(mergeWeightedAverage(targetCell.getAvgPressure(), oldCount, layers.getAvgPressure(), newCount));
        targetCell.setAvgLeaf_wetnessPct(mergeWeightedAverage(targetCell.getAvgLeaf_wetnessPct(), oldCount, layers.getAvgLeafWetnessPct(), newCount));

        targetCell.setAvgWindSpeed(mergeWeightedAverage(targetCell.getAvgWindSpeed(), oldCount, layers.getAvgWindSpeed(), newCount));
        targetCell.setAvgWindDirection(mergeWeightedAverage(targetCell.getAvgWindDirection(), oldCount, layers.getAvgWindDirection(), newCount));

        targetCell.setAvgPm25(mergeWeightedAverage(targetCell.getAvgPm25(), oldCount, layers.getAvgPm25(), newCount));
        targetCell.setAvgPm10(mergeWeightedAverage(targetCell.getAvgPm10(), oldCount, layers.getAvgPm10(), newCount));
        targetCell.setAvgPm100(mergeWeightedAverage(targetCell.getAvgPm100(), oldCount, layers.getAvgPm100(), newCount));

        targetCell.setAvgVoc(mergeWeightedAverage(targetCell.getAvgVoc(), oldCount, layers.getAvgVoc(), newCount));
        targetCell.setAvgNoiseDb(mergeWeightedAverage(targetCell.getAvgNoiseDb(), oldCount, layers.getAvgNoiseDb(), newCount));

        targetCell.setAvgRainMm(mergeWeightedAverage(targetCell.getAvgRainMm(), oldCount, layers.getAvgRainMm(), newCount));
        targetCell.setAvgSnowCm(mergeWeightedAverage(targetCell.getAvgSnowCm(), oldCount, layers.getAvgSnowCm(), newCount));
        targetCell.setAvgEvapRate(mergeWeightedAverage(targetCell.getAvgEvapRate(), oldCount, layers.getAvgEvapRate(), newCount));

        targetCell.setAvgUvIndex(mergeWeightedAverage(targetCell.getAvgUvIndex(), oldCount, layers.getAvgUvIndex(), newCount));
        targetCell.setAvgSolarRadiationWm2(mergeWeightedAverage(targetCell.getAvgSolarRadiationWm2(), oldCount, layers.getAvgSolarRadiationWm2(), newCount));
        targetCell.setAvgLux(mergeWeightedAverage(targetCell.getAvgLux(), oldCount, layers.getAvgLux(), newCount));
        targetCell.setAvgVisibilityM(mergeWeightedAverage(targetCell.getAvgVisibilityM(), oldCount, layers.getAvgVisibilityM(), newCount));
    }

    @Override
    public @NonNull GridCellLayers convertWeatherGridCellsToWeatherMap(@NonNull WeatherGridCellLayer gridCellMetric) {
        return GridCellLayers.newBuilder()

                .setGeohash(gridCellMetric.getGeohash())
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
