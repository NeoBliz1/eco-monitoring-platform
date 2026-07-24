package me.neobliz1.ecomonitoring.platform.analysis.domain.service;

import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.WeatherPacket;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.WeatherMap;
import org.apache.kafka.streams.processor.api.ProcessorContext;

import java.util.List;
import java.util.Map;

public interface TelemetryPersistentService {

    void updateRealTimeSlidingWindow(WeatherPacket packet, double latGrid, double lonGrid);

    void persistAggregatedHistory(Map<Long, Map<String, List<WeatherPacket>>> extractionMatrix,
                                  ProcessorContext<String, WeatherMap> context,
                                  long currentStreamTime);
}
