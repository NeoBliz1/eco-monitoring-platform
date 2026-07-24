package me.neobliz1.ecomonitoring.platform.analysis.domain.service;

import lombok.Getter;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.AirQualityReading;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.AmbientReading;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.OpticalReading;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.PrecipitationReading;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.SensorReading;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.WindReading;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.GridCellLayers;

import java.util.DoubleSummaryStatistics;

@Getter
public class TelemetryAnalysisAccumulator {

    private final DoubleSummaryStatistics temp = new DoubleSummaryStatistics();
    private final DoubleSummaryStatistics humidity = new DoubleSummaryStatistics();
    private final DoubleSummaryStatistics pressure = new DoubleSummaryStatistics();
    private final DoubleSummaryStatistics leafWetness = new DoubleSummaryStatistics();

    private final DoubleSummaryStatistics windSpeed = new DoubleSummaryStatistics();
    private final DoubleSummaryStatistics windSin = new DoubleSummaryStatistics();
    private final DoubleSummaryStatistics windCos = new DoubleSummaryStatistics();

    private final DoubleSummaryStatistics pm100 = new DoubleSummaryStatistics();
    private final DoubleSummaryStatistics pm25 = new DoubleSummaryStatistics();
    private final DoubleSummaryStatistics pm10 = new DoubleSummaryStatistics();
    private final DoubleSummaryStatistics voc = new DoubleSummaryStatistics();
    private final DoubleSummaryStatistics noise = new DoubleSummaryStatistics();

    private final DoubleSummaryStatistics rain = new DoubleSummaryStatistics();
    private final DoubleSummaryStatistics snow = new DoubleSummaryStatistics();
    private final DoubleSummaryStatistics evaporate = new DoubleSummaryStatistics();

    private final DoubleSummaryStatistics uv = new DoubleSummaryStatistics();
    private final DoubleSummaryStatistics solar = new DoubleSummaryStatistics();
    private final DoubleSummaryStatistics lux = new DoubleSummaryStatistics();
    private final DoubleSummaryStatistics vis = new DoubleSummaryStatistics();

    public void accumulate(SensorReading reading) {
        switch (reading.getSensorDataCase()) {
            case AMBIENT -> {
                AmbientReading r = reading.getAmbient();
                temp.accept(r.getTemperatureC());
                humidity.accept(r.getHumidityPct());
                pressure.accept(r.getPressureHpa());
                leafWetness.accept(r.getLeafWetnessPct());
            }
            case WIND -> {
                WindReading r = reading.getWind();
                windSpeed.accept(r.getSpeedMps());
                double rad = Math.toRadians(r.getDirectionDeg());
                windSin.accept(Math.sin(rad));
                windCos.accept(Math.cos(rad));
            }
            case AIR_QUALITY -> {
                AirQualityReading r = reading.getAirQuality();
                pm100.accept(r.getPm100());
                pm25.accept(r.getPm25());
                pm10.accept(r.getPm10());
                voc.accept(r.getVocIndex());
                noise.accept(r.getNoiseDb());
            }
            case PRECIPITATION -> {
                PrecipitationReading r = reading.getPrecipitation();
                rain.accept(r.getRainRateMmH());
                snow.accept(r.getSnowDepthCm());
                evaporate.accept(r.getEvaporationRate());
            }
            case OPTICAL -> {
                OpticalReading r = reading.getOptical();
                uv.accept(r.getUvIndex());
                solar.accept(r.getSolarRadiationWm2());
                lux.accept(r.getLux());
                vis.accept(r.getVisibilityM());
            }
            case SENSORDATA_NOT_SET -> {}
        }
    }

    public void merge(TelemetryAnalysisAccumulator other) {
        this.temp.combine(other.getTemp());
        this.humidity.combine(other.getHumidity());
        this.pressure.combine(other.getPressure());
        this.leafWetness.combine(other.getLeafWetness());

        this.windSpeed.combine(other.getWindSpeed());
        this.windSin.combine(other.getWindSin());
        this.windCos.combine(other.getWindCos());

        this.pm100.combine(other.getPm100());
        this.pm25.combine(other.getPm25());
        this.pm10.combine(other.getPm10());
        this.voc.combine(other.getVoc());
        this.noise.combine(other.getNoise());

        this.rain.combine(other.getRain());
        this.snow.combine(other.getSnow());
        this.evaporate.combine(other.getEvaporate());

        this.uv.combine(other.getUv());
        this.solar.combine(other.getSolar());
        this.lux.combine(other.getLux());
        this.vis.combine(other.getVis());
    }

    public GridCellLayers.Builder applyTo(GridCellLayers.Builder builder) {
        if (temp.getCount() > 0) {
            builder.setAvgTemperature(temp.getAverage());
            builder.setAvgHumidity(humidity.getAverage());
            builder.setAvgPressure(pressure.getAverage());
            builder.setAvgLeafWetnessPct(leafWetness.getAverage());
        }
        if (windSpeed.getCount() > 0) {
            builder.setAvgWindSpeed(windSpeed.getAverage());
            double avgAngleDeg = Math.toDegrees(Math.atan2(windSin.getSum() / windSin.getCount(), windCos.getSum() / windCos.getCount()));
            if (avgAngleDeg < 0) avgAngleDeg += 360.0;
            builder.setAvgWindDirection((int) Math.round(avgAngleDeg));
        }
        if (pm100.getCount() > 0) {
            builder.setAvgPm100(pm100.getAverage());
            builder.setAvgPm25(pm25.getAverage());
            builder.setAvgPm10(pm10.getAverage());
            builder.setAvgVoc(voc.getAverage());
            builder.setAvgNoiseDb(noise.getAverage());
        }
        if (rain.getCount() > 0) {
            builder.setAvgRainMm(rain.getAverage());
            builder.setAvgSnowCm(snow.getAverage());
            builder.setAvgEvapRate(evaporate.getAverage());
        }
        if (uv.getCount() > 0) {
            builder.setAvgUvIndex(uv.getAverage());
            builder.setAvgSolarRadiationWm2(solar.getAverage());
            builder.setAvgLux(lux.getAverage());
            builder.setAvgVisibilityM(vis.getAverage());
        }
        return builder;
    }
}
