package me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.messaging.kafka.processor.model;

import me.neobliz1.ecomonitoring.platform.analysis.domain.model.accumulator.PortSensorDataCase;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.SensorReading;

public record PortSensorReading(SensorReading sensorReading) {

    public PortSensorDataCase getSensorDataCase() {
        SensorReading.SensorDataCase sensorDataCase = sensorReading.getSensorDataCase();
        return PortSensorDataCase.forNumber(sensorDataCase.getNumber());
    }
}