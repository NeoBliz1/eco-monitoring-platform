package me.neobliz1.ecomonitoring.platform.ingestion.infrastructure.delivery.web;

import static me.neobliz1.ecomonitoring.platform.common.api.uri.UriConstant.BLOCKING_TELEMETRY_ENDPOINT_URI;
import static me.neobliz1.ecomonitoring.platform.common.api.uri.UriConstant.REACTIVE_TELEMETRY_ENDPOINT_URI;
import static me.neobliz1.ecomonitoring.platform.common.api.uri.UriConstant.TELEMETRY_URI;

import io.github.neobliz1.validproto.annotation.ValidProto;
import io.github.neobliz1.validproto.annotation.ValidatedProto;
import lombok.RequiredArgsConstructor;
import me.neobliz1.ecomonitoring.platform.ingestion.domain.service.TelemetryIngestionService;
import me.neobliz1.ecomonitoring.platform.model.exception.PipelineTimeoutException;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.WeatherPacket;
import org.springframework.http.HttpStatus;
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
@RequestMapping(TELEMETRY_URI)
public class TelemetryInvocationController {

    private final TelemetryIngestionService telemetryIngestionService;

    public static ResponseEntity<Void> getResponseEntity(Boolean isAccepted) {
        if(Boolean.TRUE.equals(isAccepted)) {
            return ResponseEntity.status(HttpStatus.ACCEPTED).build();
        } else {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).build();
        }
    }

    @PostMapping(value = REACTIVE_TELEMETRY_ENDPOINT_URI, consumes = MediaType.APPLICATION_PROTOBUF_VALUE)
    public Mono<ResponseEntity<Void>> receivedReactiveSensorStationData(@ValidProto @RequestBody WeatherPacket packet) {
        return telemetryIngestionService.processTelemetryPacket(packet)
                .timeout(Duration.ofMillis(200))
                .publishOn(Schedulers.parallel())
                .map(TelemetryInvocationController::getResponseEntity)
                .onErrorMap(ex -> {
                    if(ex instanceof TimeoutException) {
                        return new PipelineTimeoutException();
                    } else {
                        return ex;
                    }
                });
    }

    @PostMapping(value = BLOCKING_TELEMETRY_ENDPOINT_URI, consumes = MediaType.APPLICATION_PROTOBUF_VALUE)
    public ResponseEntity<Void> receivedSensorStationDataVirtual(@ValidProto @RequestBody WeatherPacket packet) {
        return getResponseEntity(telemetryIngestionService.processTelemetryPacketVirtual(packet));
    }
}
