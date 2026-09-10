package me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.inbound.web;

import static me.neobliz1.ecomonitoring.platform.common.api.uri.UriConstant.WEATHER_MAP_ENDPOINT;
import static me.neobliz1.ecomonitoring.platform.common.api.uri.UriConstant.WEATHER_MAP_URI;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import me.neobliz1.ecomonitoring.platform.analysis.domain.port.inbound.TelemetryQueryService;
import me.neobliz1.ecomonitoring.platform.analysis.domain.service.SpatialRequestValidator;
import me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.inbound.web.doc.WeatherMapResponse;
import me.neobliz1.ecomonitoring.platform.model.dto.ErrorEnvelopeDto;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.WeatherMap;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
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
            @ApiResponse(responseCode = "200", description = "OK", content = @Content(mediaType = "application/json", schema = @Schema(implementation = WeatherMapResponse.class))),
            @ApiResponse(responseCode = "400", description = "Bad request", content = @Content(mediaType = "application/json", schema = @Schema(implementation = ErrorEnvelopeDto.class))),
            @ApiResponse(responseCode = "404", description = "Not found", content = @Content(mediaType = "application/json", schema = @Schema(implementation = ErrorEnvelopeDto.class)))
    })
    @GetMapping(value = WEATHER_MAP_ENDPOINT, produces = MediaType.APPLICATION_JSON_VALUE)
    @Cacheable(
            value = "weatherMaps",
            key = "#targetTimestamp + '#' + #minLat + ',' + #maxLat + ',' + #minLon + ',' + #maxLon"
    )
    public ResponseEntity<WeatherMap> getWeatherMapByTimeAndCoordinatesSquare(
            @RequestParam(name = "targetTimestamp")
            @Min(value = 0L, message = "Timestamp cannot be negative")
            @Max(value = 4102444800000L, message = "Timestamp cannot be unreasonably far in the future (Max: Year 2100)")
            long targetTimestamp,
            @NonNull @RequestParam(name = "min-lat") Double minLat,
            @NonNull @RequestParam(name = "max-lat") Double maxLat,
            @NonNull @RequestParam(name = "min-lon") Double minLon,
            @NonNull @RequestParam(name = "max-lon") Double maxLon
    ) {
        SpatialRequestValidator.validateCoordinatesBox(minLat, maxLat, minLon, maxLon);
        WeatherMap mapByCoordinates = queryService.getLatestTimeIntervalWeatherMapByCoordinates(targetTimestamp, minLat,
                maxLat, minLon, maxLon);
        return ResponseEntity.ok(mapByCoordinates);
    }
}