package me.neobliz1.ecomonitoring.platform.ingestion.infrastructure.adapter.inbound.web;

import static me.neobliz1.ecomonitoring.platform.common.api.uri.UriConstant.BLOCKING_TELEMETRY_ENDPOINT_URI;
import static me.neobliz1.ecomonitoring.platform.common.api.uri.UriConstant.REACTIVE_TELEMETRY_ENDPOINT_URI;
import static me.neobliz1.ecomonitoring.platform.common.api.uri.UriConstant.TELEMETRY_URI;

import io.github.neobliz1.validproto.annotation.ValidProto;
import io.github.neobliz1.validproto.annotation.ValidatedProto;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import me.neobliz1.ecomonitoring.platform.common.docs.ValidationErrorResponse;
import me.neobliz1.ecomonitoring.platform.ingestion.domain.port.inbound.TelemetryIngestionService;
import me.neobliz1.ecomonitoring.platform.model.exception.PipelineTimeoutException;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.WeatherPacket;
import org.jetbrains.annotations.NotNull;
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
@Tag(name = "Telemetry Ingestion")
public class TelemetryInvocationController {

    private final TelemetryIngestionService telemetryIngestionService;

    private static @NotNull WeatherPacket injectTraceStringToWeatherPacket(WeatherPacket packet) {
        SpanContext activeContext = Span.current().getSpanContext();
        String traceParentString = String.format("00-%s-%s-%s",
                activeContext.getTraceId(),
                activeContext.getSpanId(),
                activeContext.getTraceFlags().asHex());
        return WeatherPacket.newBuilder(packet)
                .setTraceParent(traceParentString)
                .build();
    }

    public static ResponseEntity<Void> getResponseEntity(Boolean isAccepted) {
        if(Boolean.TRUE.equals(isAccepted)) {
            return ResponseEntity.status(HttpStatus.ACCEPTED).build();
        } else {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).build();
        }
    }

    @Operation(summary = "Ingest reactive sensor data")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "202", description = "Accepted"),
            @ApiResponse(responseCode = "400", description = "Bad request", content = @Content(mediaType = "application/json", schema = @Schema(implementation = ValidationErrorResponse.class))),
            @ApiResponse(responseCode = "504", description = "Gateway timeout")
    })
    @PostMapping(value = REACTIVE_TELEMETRY_ENDPOINT_URI, consumes = MediaType.APPLICATION_PROTOBUF_VALUE)
    public Mono<ResponseEntity<Void>> receivedReactiveSensorStationData(@ValidProto @RequestBody WeatherPacket packet) {
        WeatherPacket tracedPacket = injectTraceStringToWeatherPacket(packet);

        return telemetryIngestionService.processTelemetryPacket(tracedPacket)
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

    @Operation(summary = "Ingest blocking sensor data")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "202", description = "Accepted"),
            @ApiResponse(responseCode = "400", description = "Bad request", content = @Content(mediaType = "application/json", schema = @Schema(implementation = ValidationErrorResponse.class)))
    })
    @PostMapping(value = BLOCKING_TELEMETRY_ENDPOINT_URI, consumes = MediaType.APPLICATION_PROTOBUF_VALUE)
    public ResponseEntity<Void> receivedSensorStationDataVirtual(@ValidProto @RequestBody WeatherPacket packet) {
        WeatherPacket tracedPacket = injectTraceStringToWeatherPacket(packet);

        return getResponseEntity(telemetryIngestionService.processTelemetryPacketVirtual(tracedPacket));
    }
}
