package me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.inbound.web;

import static me.neobliz1.ecomonitoring.platform.common.api.uri.UriConstants.WEATHER_MAP_ENDPOINT;
import static me.neobliz1.ecomonitoring.platform.common.api.uri.UriConstants.WEATHER_MAP_URI;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import me.neobliz1.ecomonitoring.platform.analysis.domain.model.dto.WeatherMapAnalysisRequestQuery;
import me.neobliz1.ecomonitoring.platform.analysis.domain.port.inbound.TelemetryQueryService;
import me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.inbound.web.doc.WeatherMapResponse;
import me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.messaging.kafka.record.WeatherMapRecord;
import me.neobliz1.ecomonitoring.platform.model.dto.ErrorEnvelopeDto;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.WeatherMap;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Validated
@RestController
@RequiredArgsConstructor
@RequestMapping(WEATHER_MAP_URI)
@Tag(name = "Telemetry Analysis")
public class TelemetryAnalysisController {

    private final TelemetryQueryService queryService;

    @Operation(summary = "Query weather map matrix")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "OK", content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                    schema = @Schema(implementation = WeatherMapResponse.class))),
            @ApiResponse(responseCode = "400", description = "Bad request", content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                    schema = @Schema(implementation = ErrorEnvelopeDto.class))),
            @ApiResponse(responseCode = "404", description = "Not found", content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                    schema = @Schema(implementation = ErrorEnvelopeDto.class)))
    })
    @GetMapping(value = WEATHER_MAP_ENDPOINT, produces = MediaType.APPLICATION_JSON_VALUE)
    @Cacheable(
            value = "weatherMaps",
            key = "#query.targetTimestamp + '#' + #query.minLat + ',' + #query.maxLat + ',' + #query.minLon + ',' + #query.maxLon"
    )
    public ResponseEntity<WeatherMap> getWeatherMapByTimeAndCoordinatesSquare(@Valid WeatherMapAnalysisRequestQuery query) {
        WeatherMapRecord mapRecordByCoordinates = queryService.getLatestTimeIntervalWeatherMapByCoordinates(query);
        WeatherMap payload = mapRecordByCoordinates.payload();
        return ResponseEntity.ok(payload);
    }
}