package me.neobliz1.ecomonitoring.platform.history.infrastructure.mapper;

import me.neobliz1.ecomonitoring.platform.history.domain.model.entity.WeatherGridCellLayer;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.GridCellLayers;
import org.jspecify.annotations.NonNull;

public class GridCellLayerAggregator {

    private int oldCount;
    private int newCount;

    public void mergeNewGridCellLayerIntoExistingGridCellLayer(@NonNull GridCellLayers newGridCellLayer,
                                                               @NonNull WeatherGridCellLayer existingGridCellLayer) {
        this.oldCount = existingGridCellLayer.getReadingCount();
        this.newCount = newGridCellLayer.getReadingCount();
        int combinedCount = oldCount+newCount;
        existingGridCellLayer.setReadingCount(combinedCount);
        mergeNewCellMetricsToAmbientLayer(newGridCellLayer, existingGridCellLayer);
        mergeNewCellMetricsToWindLayer(newGridCellLayer, existingGridCellLayer);
        mergeNewCellMetricsToAirQualityLayer(newGridCellLayer, existingGridCellLayer);
        mergeNewCellMetricsToPrecipitationLayer(newGridCellLayer, existingGridCellLayer);
        mergeNewCellMetricsToOpticalLayer(newGridCellLayer, existingGridCellLayer);
    }

    private void mergeNewCellMetricsToAmbientLayer(GridCellLayers sourceLayers, WeatherGridCellLayer targetLayer) {
        targetLayer.setAvgTemperature(mergeWeightedAverage(targetLayer.getAvgTemperature(), sourceLayers.getAvgTemperature()));
        targetLayer.setAvgHumidity(mergeWeightedAverage(targetLayer.getAvgHumidity(), sourceLayers.getAvgHumidity()));
        targetLayer.setAvgPressure(mergeWeightedAverage(targetLayer.getAvgPressure(), sourceLayers.getAvgPressure()));
        targetLayer.setAvgLeaf_wetnessPct(mergeWeightedAverage(targetLayer.getAvgLeaf_wetnessPct(), sourceLayers.getAvgLeafWetnessPct()));
    }

    private void mergeNewCellMetricsToWindLayer(GridCellLayers sourceLayers, WeatherGridCellLayer targetLayer) {
        targetLayer.setAvgWindSpeed(mergeWeightedAverage(targetLayer.getAvgWindSpeed(), sourceLayers.getAvgWindSpeed()));
        targetLayer.setAvgWindDirection(mergeWeightedAverage(targetLayer.getAvgWindDirection(), sourceLayers.getAvgWindDirection()));
    }

    private void mergeNewCellMetricsToAirQualityLayer(GridCellLayers sourceLayers, WeatherGridCellLayer targetLayer) {
        targetLayer.setAvgPm25(mergeWeightedAverage(targetLayer.getAvgPm25(), sourceLayers.getAvgPm25()));
        targetLayer.setAvgPm10(mergeWeightedAverage(targetLayer.getAvgPm10(), sourceLayers.getAvgPm10()));
        targetLayer.setAvgPm100(mergeWeightedAverage(targetLayer.getAvgPm100(), sourceLayers.getAvgPm100()));
        targetLayer.setAvgVoc(mergeWeightedAverage(targetLayer.getAvgVoc(), sourceLayers.getAvgVoc()));
        targetLayer.setAvgNoiseDb(mergeWeightedAverage(targetLayer.getAvgNoiseDb(), sourceLayers.getAvgNoiseDb()));
    }

    private void mergeNewCellMetricsToPrecipitationLayer(GridCellLayers sourceLayers, WeatherGridCellLayer targetLayer) {
        targetLayer.setAvgRainMm(mergeWeightedAverage(targetLayer.getAvgRainMm(), sourceLayers.getAvgRainMm()));
        targetLayer.setAvgSnowCm(mergeWeightedAverage(targetLayer.getAvgSnowCm(), sourceLayers.getAvgSnowCm()));
        targetLayer.setAvgEvapRate(mergeWeightedAverage(targetLayer.getAvgEvapRate(), sourceLayers.getAvgEvapRate()));
    }

    private void mergeNewCellMetricsToOpticalLayer(GridCellLayers sourceLayers, WeatherGridCellLayer targetLayer) {
        targetLayer.setAvgUvIndex(mergeWeightedAverage(targetLayer.getAvgUvIndex(), sourceLayers.getAvgUvIndex()));
        targetLayer.setAvgSolarRadiationWm2(mergeWeightedAverage(targetLayer.getAvgSolarRadiationWm2(), sourceLayers.getAvgSolarRadiationWm2()));
        targetLayer.setAvgLux(mergeWeightedAverage(targetLayer.getAvgLux(), sourceLayers.getAvgLux()));
        targetLayer.setAvgVisibilityM(mergeWeightedAverage(targetLayer.getAvgVisibilityM(), sourceLayers.getAvgVisibilityM()));
    }

    private Double mergeWeightedAverage(Double oldVal, Double newVal) {
        if(oldVal==null) return newVal;
        if(oldCount<=0) return newVal;
        if(newCount<=0) return oldVal;
        return ((oldVal*oldCount)+(newVal*newCount))/(double) (oldCount+newCount);
    }
}
