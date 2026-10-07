package me.neobliz1.ecomonitoring.platform.analysis.domain.port.inbound;


import me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.messaging.kafka.processor.model.PortGridCellLayersBuilder;
import me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.messaging.kafka.processor.model.PortSensorReading;

public interface SensorGroupAccumulator {

    void accumulate(PortSensorReading reading);

    void merge(SensorGroupAccumulator other);

    void applyTo(PortGridCellLayersBuilder builder);
}