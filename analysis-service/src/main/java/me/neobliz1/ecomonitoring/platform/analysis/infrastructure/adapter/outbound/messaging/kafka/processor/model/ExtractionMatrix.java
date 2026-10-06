package me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.messaging.kafka.processor.model;

import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.WeatherPacket;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public record ExtractionMatrix(Map<Long, Map<String, List<WeatherPacket>>> spatialWeatherPacketsByTimestampContainer,
                               List<String> keysToRemove) {
    public static ExtractionMatrix empty() {
        return new ExtractionMatrix(new HashMap<>(), new ArrayList<>());
    }
}
