package me.neobliz1.ecomonitoring.platform.analysis.domain.model.accumulator;

import static me.neobliz1.ecomonitoring.platform.analysis.domain.model.accumulator.PortSensorDataCase.PRECIPITATION;

import lombok.Getter;
import lombok.val;
import me.neobliz1.ecomonitoring.platform.analysis.domain.port.inbound.SensorGroupAccumulator;
import me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.messaging.kafka.processor.model.PortGridCellLayersBuilder;
import me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.messaging.kafka.processor.model.PortSensorReading;

import java.util.DoubleSummaryStatistics;

@Getter
public class PrecipitationAccumulator implements SensorGroupAccumulator {

    private final DoubleSummaryStatistics rain = new DoubleSummaryStatistics();
    private final DoubleSummaryStatistics snow = new DoubleSummaryStatistics();
    private final DoubleSummaryStatistics evaporate = new DoubleSummaryStatistics();

    @Override
    public void accumulate(PortSensorReading reading) {
        if(reading.getSensorDataCase()!=PRECIPITATION) return;
        val r = reading.sensorReading().getPrecipitation();
        rain.accept(r.getRainRateMmH());
        snow.accept(r.getSnowDepthCm());
        evaporate.accept(r.getEvaporationRate());
    }

    @Override
    public void merge(SensorGroupAccumulator other) {
        PrecipitationAccumulator o = (PrecipitationAccumulator) other;
        this.rain.combine(o.rain);
        this.snow.combine(o.snow);
        this.evaporate.combine(o.evaporate);
    }

    @Override
    public void applyTo(PortGridCellLayersBuilder builder) {
        if(rain.getCount()>0) {
            builder.setAvgRainMm(rain.getAverage());
            builder.setAvgSnowCm(snow.getAverage());
            builder.setAvgEvapRate(evaporate.getAverage());
        }
    }
}