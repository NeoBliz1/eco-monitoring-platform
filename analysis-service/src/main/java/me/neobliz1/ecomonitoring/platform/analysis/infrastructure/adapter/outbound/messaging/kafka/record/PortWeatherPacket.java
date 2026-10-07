package me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.messaging.kafka.record;

import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.WeatherPacket;

public record PortWeatherPacket(WeatherPacket weatherPacket, double latGrid, double lonGrid) {
}