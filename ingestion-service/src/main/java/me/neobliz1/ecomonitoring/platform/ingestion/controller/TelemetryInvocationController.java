package me.neobliz1.ecomonitoring.platform.ingestion.controller;

import static me.neobliz1.ecomonitoring.platform.ingestion.service.impl.TelemetryIngestionServiceImpl.getResponseEntity;

import io.github.neobliz1.validproto.annotation.ValidProto;
import io.github.neobliz1.validproto.annotation.ValidatedProto;
import lombok.RequiredArgsConstructor;
import me.neobliz1.ecomonitoring.platform.ingestion.service.TelemetryIngestionService;
import me.neobliz1.ecomonitoring.platform.ingestion.service.impl.TelemetryIngestionServiceImpl;
import me.neobliz1.ecomonitoring.platform.model.exception.PipelineTimeoutException;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.WeatherPacket;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.concurrent.TimeoutException;

@ValidatedProto
@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/telemetry")
public class TelemetryInvocationController {

    private final TelemetryIngestionService telemetryIngestionService;

    @PostMapping(value = "/mono", consumes = MediaType.APPLICATION_PROTOBUF_VALUE)
    public Mono<ResponseEntity<Void>> receivedReactiveSensorStationData(@ValidProto @RequestBody WeatherPacket packet) {
        return telemetryIngestionService.processTelemetryPacket(packet)
                .timeout(Duration.ofMillis(200))
                .publishOn(Schedulers.parallel())
                .map(TelemetryIngestionServiceImpl::getResponseEntity)
                .onErrorMap(ex -> {
                    if(ex instanceof TimeoutException) {
                        return new PipelineTimeoutException();
                    } else {
                        return ex;
                    }
                });
    }

    @PostMapping(value = "/virtual", consumes = MediaType.APPLICATION_PROTOBUF_VALUE)
    public ResponseEntity<Void> receivedSensorStationDataVirtual(@ValidProto @RequestBody WeatherPacket packet) {
        return getResponseEntity(telemetryIngestionService.processTelemetryPacketVirtual(packet));
    }
}
