package me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.messaging.kafka.record;

import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.WeatherMap;

public record WeatherMapRecord(String key, WeatherMap payload) {
}