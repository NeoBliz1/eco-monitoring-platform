package me.neobliz1.ecomonitoring.platform.history.infrastructure.mapper;

import static me.neobliz1.ecomonitoring.platform.history.domain.port.service.HistoricalUtils.getBucketIdFromWeatherMap;

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

    @Override
    public void mergeTelemetryInBatch(@NonNull WeatherMap weatherMap, @NonNull WeatherMapBucket bucket) {
        if(weatherMap.getGridCellsMap().isEmpty()) {
            return;
        }
        List<WeatherGridCellLayer> existingGridCells = getExistingGridCells(weatherMap);
        Map<String, WeatherGridCellLayer> existingCellsMap = Map.of();
        GridCellLayerAggregator gridCellLayerAggregator = null;
        if(!existingGridCells.isEmpty()) {
            gridCellLayerAggregator = new GridCellLayerAggregator();
            existingCellsMap = convertExistsGridCellsSetToMap(existingGridCells);
        }
        List<WeatherGridCellLayer> gridCellLayersToSave = new ArrayList<>();
        for(Map.Entry<String, GridCellLayers> entry : weatherMap.getGridCellsMap().entrySet()) {
            String geohashKey = entry.getKey();
            GridCellLayers gridCellLayer = entry.getValue();
            if(gridCellLayer==null) continue;
            WeatherGridCellLayer existingGridCellLayer = existingCellsMap.get(geohashKey);
            if(existingGridCellLayer!=null && gridCellLayerAggregator!=null) {
                gridCellLayerAggregator.mergeNewGridCellLayerIntoExistingGridCellLayer(gridCellLayer, existingGridCellLayer);
                gridCellLayersToSave.add(existingGridCellLayer);
            } else {
                WeatherGridCellLayer newGridCellLayer = createWeatherGridCellLayer(gridCellLayer, bucket, geohashKey);
                gridCellLayersToSave.add(newGridCellLayer);
            }
        }
        if(!gridCellLayersToSave.isEmpty()) {
            gridCellJpaRepository.saveAllAndFlush(gridCellLayersToSave);
        }
    }

    private @NonNull Map<String, WeatherGridCellLayer> convertExistsGridCellsSetToMap(@NonNull List<WeatherGridCellLayer> existingGridCells) {
        Map<String, WeatherGridCellLayer> gridCellLayers = new HashMap<>();
        for(WeatherGridCellLayer layer : existingGridCells) {
            gridCellLayers.put(layer.getGeohash(), layer);
        }
        return gridCellLayers;
    }

    private @NonNull List<WeatherGridCellLayer> getExistingGridCells(@NonNull WeatherMap weatherMap) {
        Set<String> geohashes = weatherMap.getGridCellsMap().keySet();
        return gridCellJpaRepository.findSpecificGridCellLayersForMerge(getBucketIdFromWeatherMap(weatherMap), geohashes);
    }

    private @NonNull WeatherGridCellLayer createWeatherGridCellLayer(@NonNull GridCellLayers gridCellsLayer,
                                                                     @NonNull WeatherMapBucket bucket,
                                                                     @NonNull String geohashKey) {
        WeatherGridCellLayer newGridCellLayer = new WeatherGridCellLayer(bucket, geohashKey);
        newGridCellLayer.setBucketId(bucket.getId());
        newGridCellLayer.setGeohash(geohashKey);
        newGridCellLayer.setReadingCount(gridCellsLayer.getReadingCount());
        setNewCellMetricsToAmbientLayer(newGridCellLayer, gridCellsLayer);
        setNewCellMetricsToWindLayer(newGridCellLayer, gridCellsLayer);
        setNewCellMetricsToAirQualityLayer(newGridCellLayer, gridCellsLayer);
        setNewCellMetricsToPrecipitationLayer(newGridCellLayer, gridCellsLayer);
        setNewCellMetricsToOpticalLayer(newGridCellLayer, gridCellsLayer);
        return newGridCellLayer;
    }

    private void setNewCellMetricsToAmbientLayer(@NonNull WeatherGridCellLayer newCellMetric, @NonNull GridCellLayers gridCellsLayer) {
        newCellMetric.setAvgTemperature(gridCellsLayer.getAvgTemperature());
        newCellMetric.setAvgHumidity(gridCellsLayer.getAvgHumidity());
        newCellMetric.setAvgPressure(gridCellsLayer.getAvgPressure());
        newCellMetric.setAvgLeaf_wetnessPct(gridCellsLayer.getAvgLeafWetnessPct());
    }

    private void setNewCellMetricsToWindLayer(@NonNull WeatherGridCellLayer newCellMetric, @NonNull GridCellLayers gridCellsLayer) {
        newCellMetric.setAvgWindSpeed(gridCellsLayer.getAvgWindSpeed());
        newCellMetric.setAvgWindDirection(gridCellsLayer.getAvgWindDirection());
    }

    private void setNewCellMetricsToAirQualityLayer(@NonNull WeatherGridCellLayer newCellMetric, @NonNull GridCellLayers gridCellsLayer) {
        newCellMetric.setAvgPm25(gridCellsLayer.getAvgPm25());
        newCellMetric.setAvgPm10(gridCellsLayer.getAvgPm10());
        newCellMetric.setAvgPm100(gridCellsLayer.getAvgPm100());
        newCellMetric.setAvgVoc(gridCellsLayer.getAvgVoc());
        newCellMetric.setAvgNoiseDb(gridCellsLayer.getAvgNoiseDb());
    }

    private void setNewCellMetricsToPrecipitationLayer(@NonNull WeatherGridCellLayer newCellMetric, @NonNull GridCellLayers gridCellsLayer) {
        newCellMetric.setAvgRainMm(gridCellsLayer.getAvgRainMm());
        newCellMetric.setAvgSnowCm(gridCellsLayer.getAvgSnowCm());
        newCellMetric.setAvgEvapRate(gridCellsLayer.getAvgEvapRate());
    }

    private void setNewCellMetricsToOpticalLayer(@NonNull WeatherGridCellLayer newCellMetric, @NonNull GridCellLayers gridCellsLayer) {
        newCellMetric.setAvgUvIndex(gridCellsLayer.getAvgUvIndex());
        newCellMetric.setAvgSolarRadiationWm2(gridCellsLayer.getAvgSolarRadiationWm2());
        newCellMetric.setAvgLux(gridCellsLayer.getAvgLux());
        newCellMetric.setAvgVisibilityM(gridCellsLayer.getAvgVisibilityM());
    }

    @Override
    public @NonNull GridCellLayers convertWeatherGridCellsToWeatherMap(@NonNull WeatherGridCellLayer gridCellMetric) {
        GridCellLayers.Builder builder = GridCellLayers.newBuilder()
                .setGeohash(gridCellMetric.getGeohash())
                .setReadingCount(gridCellMetric.getReadingCount());
        setAmbientLayerToBuilder(gridCellMetric, builder);
        setWindLayerToBuilder(gridCellMetric, builder);
        setAirQualityLayerToBuilder(gridCellMetric, builder);
        setPrecipitationLayerToBuilder(gridCellMetric, builder);
        setOpticalLayerToBuilder(gridCellMetric, builder);
        return builder.build();
    }

    private void setAmbientLayerToBuilder(@NonNull WeatherGridCellLayer gridCellMetric, GridCellLayers.Builder builder) {
        builder.setAvgTemperature(gridCellMetric.getAvgTemperature())
                .setAvgHumidity(gridCellMetric.getAvgHumidity())
                .setAvgPressure(gridCellMetric.getAvgPressure())
                .setAvgLeafWetnessPct(gridCellMetric.getAvgLeaf_wetnessPct());
    }

    private void setWindLayerToBuilder(@NonNull WeatherGridCellLayer gridCellMetric, GridCellLayers.Builder builder) {
        builder.setAvgWindSpeed(gridCellMetric.getAvgWindSpeed())
                .setAvgWindDirection(gridCellMetric.getAvgWindDirection());
    }

    private void setAirQualityLayerToBuilder(@NonNull WeatherGridCellLayer gridCellMetric, GridCellLayers.Builder builder) {
        builder.setAvgPm25(gridCellMetric.getAvgPm25())
                .setAvgPm10(gridCellMetric.getAvgPm10())
                .setAvgPm100(gridCellMetric.getAvgPm100())
                .setAvgVoc(gridCellMetric.getAvgVoc())
                .setAvgNoiseDb(gridCellMetric.getAvgNoiseDb());
    }

    private void setPrecipitationLayerToBuilder(@NonNull WeatherGridCellLayer gridCellMetric, GridCellLayers.Builder builder) {
        builder.setAvgRainMm(gridCellMetric.getAvgRainMm())
                .setAvgSnowCm(gridCellMetric.getAvgSnowCm())
                .setAvgEvapRate(gridCellMetric.getAvgEvapRate());
    }

    private void setOpticalLayerToBuilder(@NonNull WeatherGridCellLayer gridCellMetric, GridCellLayers.Builder builder) {
        builder.setAvgUvIndex(gridCellMetric.getAvgUvIndex())
                .setAvgSolarRadiationWm2(gridCellMetric.getAvgSolarRadiationWm2())
                .setAvgLux(gridCellMetric.getAvgLux())
                .setAvgVisibilityM(gridCellMetric.getAvgVisibilityM());
    }
}
