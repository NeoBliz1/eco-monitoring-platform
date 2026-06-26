package me.neobliz1.ecomonitoring.platform.ingestion.service;

import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.WeatherPacket;
import reactor.core.publisher.Mono;

public interface TelemetryIngestionService {

    Mono<Boolean> processTelemetryPacket(WeatherPacket packet);
    boolean processTelemetryPacketVirtual(WeatherPacket packet);
}
