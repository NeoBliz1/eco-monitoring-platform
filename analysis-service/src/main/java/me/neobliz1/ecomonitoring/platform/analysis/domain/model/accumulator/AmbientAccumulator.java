package me.neobliz1.ecomonitoring.platform.analysis.domain.model.accumulator;

import static me.neobliz1.ecomonitoring.platform.analysis.domain.model.accumulator.PortSensorDataCase.AMBIENT;

import lombok.Getter;
import lombok.val;
import me.neobliz1.ecomonitoring.platform.analysis.domain.port.inbound.SensorGroupAccumulator;
import me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.messaging.kafka.processor.model.PortGridCellLayersBuilder;
import me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.messaging.kafka.processor.model.PortSensorReading;

import java.util.DoubleSummaryStatistics;

@Getter
public class AmbientAccumulator implements SensorGroupAccumulator {

    private final DoubleSummaryStatistics temp = new DoubleSummaryStatistics();
    private final DoubleSummaryStatistics humidity = new DoubleSummaryStatistics();
    private final DoubleSummaryStatistics pressure = new DoubleSummaryStatistics();
    private final DoubleSummaryStatistics leafWetness = new DoubleSummaryStatistics();

    @Override
    public void accumulate(PortSensorReading reading) {
        if(reading.getSensorDataCase()!=AMBIENT) return;
        val r = reading.sensorReading().getAmbient();
        temp.accept(r.getTemperatureC());
        humidity.accept(r.getHumidityPct());
        pressure.accept(r.getPressureHpa());
        leafWetness.accept(r.getLeafWetnessPct());
    }

    @Override
    public void merge(SensorGroupAccumulator other) {
        AmbientAccumulator o = (AmbientAccumulator) other;
        this.temp.combine(o.temp);
        this.humidity.combine(o.humidity);
        this.pressure.combine(o.pressure);
        this.leafWetness.combine(o.leafWetness);
    }

    @Override
    public void applyTo(PortGridCellLayersBuilder builder) {
        if(temp.getCount()>0) {
            builder.setAvgTemperature(temp.getAverage());
            builder.setAvgHumidity(humidity.getAverage());
            builder.setAvgPressure(pressure.getAverage());
            builder.setAvgLeafWetnessPct(leafWetness.getAverage());
        }
    }
}