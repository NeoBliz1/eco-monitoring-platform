package me.neobliz1.ecomonitoring.platform.analysis.domain.service;

import lombok.Getter;
import me.neobliz1.ecomonitoring.platform.analysis.domain.model.accumulator.AirQualityAccumulator;
import me.neobliz1.ecomonitoring.platform.analysis.domain.model.accumulator.AmbientAccumulator;
import me.neobliz1.ecomonitoring.platform.analysis.domain.model.accumulator.OpticalAccumulator;
import me.neobliz1.ecomonitoring.platform.analysis.domain.model.accumulator.PrecipitationAccumulator;
import me.neobliz1.ecomonitoring.platform.analysis.domain.model.accumulator.WindAccumulator;
import me.neobliz1.ecomonitoring.platform.analysis.domain.port.inbound.SensorGroupAccumulator;
import me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.messaging.kafka.processor.model.PortGridCellLayersBuilder;
import me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.messaging.kafka.processor.model.PortSensorReading;

import java.util.List;

@Getter
public class TelemetryAnalysisAccumulator {

    private final List<SensorGroupAccumulator> groups = List.of(
            new AmbientAccumulator(),
            new WindAccumulator(),
            new AirQualityAccumulator(),
            new PrecipitationAccumulator(),
            new OpticalAccumulator()
    );

    public void accumulate(PortSensorReading reading) {
        for(SensorGroupAccumulator groupAccumulator : groups) {
            groupAccumulator.accumulate(reading);
        }
    }

    public void merge(TelemetryAnalysisAccumulator other) {
        for(int i = 0; i<groups.size(); i++) {
            groups.get(i).merge(other.groups.get(i));
        }
    }

    public PortGridCellLayersBuilder applyTo(PortGridCellLayersBuilder builder) {
        for(SensorGroupAccumulator groupAccumulator : groups) {
            groupAccumulator.applyTo(builder);
        }
        return builder;
    }
}