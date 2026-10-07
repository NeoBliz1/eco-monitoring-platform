package me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.messaging.kafka.processor.model;

import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.GridCellLayers;

public record PortGridCellLayersBuilder(GridCellLayers.Builder gridCellLayersBuilder) {

    public void setAvgTemperature(double avgTemperature) {
        gridCellLayersBuilder.setAvgTemperature(avgTemperature);
    }

    public void setAvgHumidity(double avgHumidity) {
        gridCellLayersBuilder.setAvgHumidity(avgHumidity);
    }

    public void setAvgPressure(double avgPressure) {
        gridCellLayersBuilder.setAvgPressure(avgPressure);
    }

    public void setAvgLeafWetnessPct(double avgLeafWetnessPct) {
        gridCellLayersBuilder.setAvgLeafWetnessPct(avgLeafWetnessPct);
    }

    public void setAvgWindSpeed(double avgWindSpeed) {
        gridCellLayersBuilder.setAvgWindSpeed(avgWindSpeed);
    }

    public void setAvgWindDirection(int avgWindDirection) {
        gridCellLayersBuilder.setAvgWindDirection(avgWindDirection);
    }

    public void setAvgPm25(double avgPm25) {
        gridCellLayersBuilder.setAvgPm25(avgPm25);
    }

    public void setAvgPm10(double avgPm10) {
        gridCellLayersBuilder.setAvgPm10(avgPm10);
    }

    public void setAvgPm100(double avgPm100) {
        gridCellLayersBuilder.setAvgPm100(avgPm100);
    }

    public void setAvgVoc(double avgVoc) {
        gridCellLayersBuilder.setAvgVoc(avgVoc);
    }

    public void setAvgNoiseDb(double avgNoiseDb) {
        gridCellLayersBuilder.setAvgNoiseDb(avgNoiseDb);
    }

    public void setAvgRainMm(double avgRainMm) {
        gridCellLayersBuilder.setAvgRainMm(avgRainMm);
    }

    public void setAvgSnowCm(double avgSnowCm) {
        gridCellLayersBuilder.setAvgSnowCm(avgSnowCm);
    }

    public void setAvgEvapRate(double avgEvapRate) {
        gridCellLayersBuilder.setAvgEvapRate(avgEvapRate);
    }

    public void setAvgUvIndex(double avgUvIndex) {
        gridCellLayersBuilder.setAvgUvIndex(avgUvIndex);
    }

    public void setAvgSolarRadiationWm2(double avgSolarRadiationWm2) {
        gridCellLayersBuilder.setAvgSolarRadiationWm2(avgSolarRadiationWm2);
    }

    public void setAvgLux(double avgLux) {
        gridCellLayersBuilder.setAvgLux(avgLux);
    }

    public void setAvgVisibilityM(double avgVisibilityM) {
        gridCellLayersBuilder.setAvgVisibilityM(avgVisibilityM);
    }
}