package me.neobliz1.ecomonitoring.platform.analysis.domain.port.outbound;

import me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.messaging.kafka.record.WeatherMapRecord;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.WeatherPacket;

import java.util.List;
import java.util.Map;

public interface TelemetryPersistentService {

    void updateRealTimeSlidingWindow(WeatherPacket packet, double latGrid, double lonGrid);

    List<WeatherMapRecord> processAndComputeAggregatedHistory(Map<Long, Map<String, List<WeatherPacket>>> extractionMatrix);
}
