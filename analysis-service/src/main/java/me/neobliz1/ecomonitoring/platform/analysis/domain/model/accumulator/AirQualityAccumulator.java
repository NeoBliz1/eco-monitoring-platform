package me.neobliz1.ecomonitoring.platform.analysis.domain.model.accumulator;

import static me.neobliz1.ecomonitoring.platform.analysis.domain.model.accumulator.PortSensorDataCase.AIR_QUALITY;

import lombok.Getter;
import lombok.val;
import me.neobliz1.ecomonitoring.platform.analysis.domain.port.inbound.SensorGroupAccumulator;
import me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.messaging.kafka.processor.model.PortGridCellLayersBuilder;
import me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.messaging.kafka.processor.model.PortSensorReading;

import java.util.DoubleSummaryStatistics;

@Getter
public class AirQualityAccumulator implements SensorGroupAccumulator {

    private final DoubleSummaryStatistics pm100 = new DoubleSummaryStatistics();
    private final DoubleSummaryStatistics pm25 = new DoubleSummaryStatistics();
    private final DoubleSummaryStatistics pm10 = new DoubleSummaryStatistics();
    private final DoubleSummaryStatistics voc = new DoubleSummaryStatistics();
    private final DoubleSummaryStatistics noise = new DoubleSummaryStatistics();

    @Override
    public void accumulate(PortSensorReading reading) {
        if(reading.getSensorDataCase()!=AIR_QUALITY) return;
        val r = reading.sensorReading().getAirQuality();
        pm100.accept(r.getPm100());
        pm25.accept(r.getPm25());
        pm10.accept(r.getPm10());
        voc.accept(r.getVocIndex());
        noise.accept(r.getNoiseDb());
    }

    @Override
    public void merge(SensorGroupAccumulator other) {
        AirQualityAccumulator o = (AirQualityAccumulator) other;
        this.pm100.combine(o.pm100);
        this.pm25.combine(o.pm25);
        this.pm10.combine(o.pm10);
        this.voc.combine(o.voc);
        this.noise.combine(o.noise);
    }

    @Override
    public void applyTo(PortGridCellLayersBuilder builder) {
        if(pm100.getCount()>0) {
            builder.setAvgPm100(pm100.getAverage());
            builder.setAvgPm25(pm25.getAverage());
            builder.setAvgPm10(pm10.getAverage());
            builder.setAvgVoc(voc.getAverage());
            builder.setAvgNoiseDb(noise.getAverage());
        }
    }
}