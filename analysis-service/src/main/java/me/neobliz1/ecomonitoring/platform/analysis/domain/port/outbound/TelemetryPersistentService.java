package me.neobliz1.ecomonitoring.platform.analysis.domain.port.outbound;

import me.neobliz1.ecomonitoring.platform.analysis.domain.service.TelemetryStatePersister;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.WeatherPacket;

import java.util.List;
import java.util.Map;

public interface TelemetryPersistentService {

    void updateRealTimeSlidingWindow(WeatherPacket packet, double latGrid, double lonGrid);
    List<TelemetryStatePersister.WeatherMapRecord> processAndComputeAggregatedHistory(Map<Long, Map<String, List<WeatherPacket>>> extractionMatrix);
}
