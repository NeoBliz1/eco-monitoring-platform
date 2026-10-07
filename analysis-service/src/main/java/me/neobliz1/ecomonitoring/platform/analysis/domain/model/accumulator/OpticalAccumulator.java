package me.neobliz1.ecomonitoring.platform.analysis.domain.model.accumulator;

import static me.neobliz1.ecomonitoring.platform.analysis.domain.model.accumulator.PortSensorDataCase.OPTICAL;

import lombok.Getter;
import lombok.val;
import me.neobliz1.ecomonitoring.platform.analysis.domain.port.inbound.SensorGroupAccumulator;
import me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.messaging.kafka.processor.model.PortGridCellLayersBuilder;
import me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.messaging.kafka.processor.model.PortSensorReading;

import java.util.DoubleSummaryStatistics;

@Getter
public class OpticalAccumulator implements SensorGroupAccumulator {

    private final DoubleSummaryStatistics uv = new DoubleSummaryStatistics();
    private final DoubleSummaryStatistics solar = new DoubleSummaryStatistics();
    private final DoubleSummaryStatistics lux = new DoubleSummaryStatistics();
    private final DoubleSummaryStatistics vis = new DoubleSummaryStatistics();

    @Override
    public void accumulate(PortSensorReading reading) {
        if(reading.getSensorDataCase()!=OPTICAL) return;
        val r = reading.sensorReading().getOptical();
        uv.accept(r.getUvIndex());
        solar.accept(r.getSolarRadiationWm2());
        lux.accept(r.getLux());
        vis.accept(r.getVisibilityM());
    }

    @Override
    public void merge(SensorGroupAccumulator other) {
        OpticalAccumulator o = (OpticalAccumulator) other;
        this.uv.combine(o.uv);
        this.solar.combine(o.solar);
        this.lux.combine(o.lux);
        this.vis.combine(o.vis);
    }

    @Override
    public void applyTo(PortGridCellLayersBuilder builder) {
        if(uv.getCount()>0) {
            builder.setAvgUvIndex(uv.getAverage());
            builder.setAvgSolarRadiationWm2(solar.getAverage());
            builder.setAvgLux(lux.getAverage());
            builder.setAvgVisibilityM(vis.getAverage());
        }
    }
}