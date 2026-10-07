package me.neobliz1.ecomonitoring.platform.analysis.domain.port.outbound;

import me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.messaging.kafka.processor.model.ExtractionMatrix;
import me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.messaging.kafka.record.PortWeatherPacket;
import me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.messaging.kafka.record.WeatherMapRecord;

import java.util.List;

public interface TelemetryPersistentService {

    void updateRealTimeSlidingWindow(PortWeatherPacket portWeatherPacket);

    List<WeatherMapRecord> processAndComputeAggregatedHistory(ExtractionMatrix extractionMatrix);
}
