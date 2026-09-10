package me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter.inbound.web;

import static me.neobliz1.ecomonitoring.platform.common.api.uri.UriConstant.TX_ID_INGESTION_HISTORY_URI;
import static me.neobliz1.ecomonitoring.platform.common.api.uri.UriConstant.WEATHER_PACKET_TX_ID_INGESTION;
import static me.neobliz1.ecomonitoring.platform.common.constant.PlatformConstants.TX_CHAIN_CONFIRMATION_PROFILE;

import io.github.neobliz1.validproto.annotation.ValidProto;
import io.github.neobliz1.validproto.annotation.ValidatedProto;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import me.neobliz1.ecomonitoring.platform.common.docs.ValidationErrorResponse;
import me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter.outbound.persistence.postgres.HistoricalTxIdRepositoryAdapter;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.WeatherPacket;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@ValidatedProto
@RestController
@RequiredArgsConstructor
@Profile(TX_CHAIN_CONFIRMATION_PROFILE)
@RequestMapping(TX_ID_INGESTION_HISTORY_URI)
@Tag(name = "Telemetry Transaction History")
public class TelemetryTransactionHistoryController {

    private final HistoricalTxIdRepositoryAdapter historicalTxIdRepositoryAdapter;

    @Operation(summary = "Ingest weather packet with transaction tracking")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "202", description = "Accepted"),
            @ApiResponse(responseCode = "400", description = "Bad request", content = @Content(mediaType = "application/json", schema = @Schema(implementation = ValidationErrorResponse.class)))
    })
    @PostMapping(value = WEATHER_PACKET_TX_ID_INGESTION, consumes = MediaType.APPLICATION_PROTOBUF_VALUE)
    public ResponseEntity<Void> ingestWeatherPacketWithTxTracking(@ValidProto @RequestBody WeatherPacket packet) {
        try {
            historicalTxIdRepositoryAdapter.processTxIdIngestionPacket(packet);
            return ResponseEntity.status(HttpStatus.ACCEPTED).build();
        } catch(Exception e) {
            log.error("Failed to persist weather packet with transaction tracking. Error: {}", e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).build();
        }
    }
}
