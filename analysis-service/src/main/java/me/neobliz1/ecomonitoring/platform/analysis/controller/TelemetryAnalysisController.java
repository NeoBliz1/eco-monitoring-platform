package me.neobliz1.ecomonitoring.platform.analysis.controller;

import static me.neobliz1.ecomonitoring.platform.common.api.uri.UriConstant.LATEST_WEATHER_MAP_ENDPOINT;
import static me.neobliz1.ecomonitoring.platform.common.api.uri.UriConstant.WEATHER_MAP_URI;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Size;
import lombok.RequiredArgsConstructor;
import me.neobliz1.ecomonitoring.platform.analysis.service.TelemetryAnalysisService;
import me.neobliz1.ecomonitoring.platform.analysis.validator.SpatialRequestValidator;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@Validated
@RestController
@RequiredArgsConstructor
@RequestMapping(WEATHER_MAP_URI)
public class TelemetryAnalysisController {

    private final TelemetryAnalysisService telemetryAnalysisService;

    @GetMapping(value = LATEST_WEATHER_MAP_ENDPOINT, produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> getLatestFiveMinuteWeatherMapJson(
            @RequestParam("targetTimestamp")
            @Min(value = 0L, message = "Timestamp cannot be negative")
            @Max(value = 4102444800000L, message = "Timestamp cannot be unreasonably far in the future (Max: Year 2100)")
            long targetTimestamp,
            @RequestParam("coordinates-square")
            @NotEmpty(message = "Coordinates square cannot be empty")
            @Size(min = 4, max = 4, message = "Coordinates square must contain exactly 4 parameters: minLat, maxLat, minLon, maxLon")
            List<Double> coordinatesSquare) {
        SpatialRequestValidator.validateCoordinatesBox(coordinatesSquare);

        return ResponseEntity.ok(telemetryAnalysisService.getLatestTimeIntervalWeatherMapByCoordinates(targetTimestamp, coordinatesSquare));
    }
}