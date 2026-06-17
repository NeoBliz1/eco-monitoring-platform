package me.neobliz1.ecomonitoring.platform.ingestion.controller;

import lombok.RequiredArgsConstructor;
import me.neobliz1.ecomonitoring.platform.ingestion.service.TelemetryIngestionService;
import me.neobliz1.ecomonitoring.platform.model.exception.PipelineTimeoutException;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.WeatherPacket;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.concurrent.TimeoutException;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/telemetry")
public class TelemetryInvocationController {

    private final TelemetryIngestionService telemetryIngestionService;

    @PostMapping(consumes = "application/x-protobuf")
    public Mono<ResponseEntity<Void>> receivedSensorStationData(WeatherPacket packet) {
        return telemetryIngestionService.processTelemetryPacket(packet)
                .timeout(Duration.ofMillis(200))
                .publishOn(Schedulers.parallel())
                .map(isAccepted -> {
                    if (Boolean.TRUE.equals(isAccepted)) {
                        return ResponseEntity.status(HttpStatus.ACCEPTED).<Void>build();
                    } else {
                        return ResponseEntity.status(HttpStatus.BAD_REQUEST).<Void>build();
                    }
                })
                .onErrorMap(ex -> {
                    if(ex instanceof TimeoutException) {
                        return new PipelineTimeoutException("Vector sidecar deadline exceeded");
                    } else {
                        return ex;
                    }
                });
    }
}
