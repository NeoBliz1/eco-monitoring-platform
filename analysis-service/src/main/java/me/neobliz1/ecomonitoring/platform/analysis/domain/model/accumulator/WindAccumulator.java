package me.neobliz1.ecomonitoring.platform.analysis.domain.model.accumulator;

import static me.neobliz1.ecomonitoring.platform.analysis.domain.model.accumulator.PortSensorDataCase.WIND;

import lombok.Getter;
import lombok.val;
import me.neobliz1.ecomonitoring.platform.analysis.domain.port.inbound.SensorGroupAccumulator;
import me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.messaging.kafka.processor.model.PortGridCellLayersBuilder;
import me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.messaging.kafka.processor.model.PortSensorReading;

import java.util.DoubleSummaryStatistics;

@Getter
public class WindAccumulator implements SensorGroupAccumulator {

    private final DoubleSummaryStatistics windSpeed = new DoubleSummaryStatistics();
    private final DoubleSummaryStatistics windSin = new DoubleSummaryStatistics();
    private final DoubleSummaryStatistics windCos = new DoubleSummaryStatistics();

    @Override
    public void accumulate(PortSensorReading reading) {
        if(reading.getSensorDataCase()!=WIND) return;
        val r = reading.sensorReading().getWind();
        windSpeed.accept(r.getSpeedMps());
        double rad = Math.toRadians(r.getDirectionDeg());
        windSin.accept(Math.sin(rad));
        windCos.accept(Math.cos(rad));
    }

    @Override
    public void merge(SensorGroupAccumulator other) {
        WindAccumulator o = (WindAccumulator) other;
        this.windSpeed.combine(o.windSpeed);
        this.windSin.combine(o.windSin);
        this.windCos.combine(o.windCos);
    }

    @Override
    public void applyTo(PortGridCellLayersBuilder builder) {
        if(windSpeed.getCount()>0) {
            builder.setAvgWindSpeed(windSpeed.getAverage());
            double avgAngleDeg = Math.toDegrees(Math.atan2(
                    windSin.getSum()/windSin.getCount(),
                    windCos.getSum()/windCos.getCount()));
            if(avgAngleDeg<0) avgAngleDeg += 360.0;
            builder.setAvgWindDirection((int) Math.round(avgAngleDeg));
        }
    }
}