package me.neobliz1.ecomonitoring.platform.ingestion.infrastructure.adapter.inbound.web;

import static me.neobliz1.ecomonitoring.platform.common.api.uri.UriConstant.BLOCKING_TELEMETRY_ENDPOINT_URI;
import static me.neobliz1.ecomonitoring.platform.common.api.uri.UriConstant.REACTIVE_TELEMETRY_ENDPOINT_URI;
import static me.neobliz1.ecomonitoring.platform.common.api.uri.UriConstant.TELEMETRY_URI;

import io.github.neobliz1.validproto.annotation.ValidProto;
import io.github.neobliz1.validproto.annotation.ValidatedProto;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import me.neobliz1.ecomonitoring.platform.ingestion.domain.port.inbound.TelemetryIngestionService;
import me.neobliz1.ecomonitoring.platform.ingestion.infrastructure.adapter.inbound.web.docs.ValidationErrorResponse;
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
@Tag(name = "Telemetry Ingestion", description = "High-throughput pipelines for multi-sensor climatic logs")
public class TelemetryInvocationController {

    private final TelemetryIngestionService telemetryIngestionService;

    @Operation(
            summary = "Ingest Reactive Sensor Data",
            description = "Asynchronously processes incoming streaming climatic packages with a strict 200ms timeout barrier boundary constraint."
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "202", description = "Payload processed and accepted into pipeline successfully", content = @Content),
            @ApiResponse(
                    responseCode = "400",
                    description = "Malformed Protobuf payload structure or validation rules failed",
                    content = @Content(mediaType = "application/json", schema = @Schema(implementation = ValidationErrorResponse.class))
            ),
            @ApiResponse(
                    responseCode = "504",
                    description = "Pipeline Timeout Error - Processing exceeded designated thread response window limit",
                    content = @Content
            )
    })
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

    @Operation(
            summary = "Ingest Virtual-Thread Sensor Data",
            description = "Handles incoming station data over blocking execution patterns routed directly across underlying virtual thread allocations."
    )
    @ApiResponses(value = {
            @ApiResponse(
                    responseCode = "202",
                    description = "Station data committed into storage buffer completely",
                    content = @Content
            ),
            @ApiResponse(
                    responseCode = "400",
                    description = "Malformed Protobuf payload structure or validation rules failed",
                    content = @Content(mediaType = "application/json", schema = @Schema(implementation = ValidationErrorResponse.class))
            )
    })
    @PostMapping(value = BLOCKING_TELEMETRY_ENDPOINT_URI, consumes = MediaType.APPLICATION_PROTOBUF_VALUE)
    public ResponseEntity<Void> receivedSensorStationDataVirtual(@ValidProto @RequestBody WeatherPacket packet) {
        return getResponseEntity(telemetryIngestionService.processTelemetryPacketVirtual(packet));
    }

    public static ResponseEntity<Void> getResponseEntity(Boolean isAccepted) {
        if(Boolean.TRUE.equals(isAccepted)) {
            return ResponseEntity.status(HttpStatus.ACCEPTED).build();
        } else {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).build();
        }
    }
}
