package me.neobliz1.ecomonitoring.platform.analysis.controller;

import lombok.RequiredArgsConstructor;
import me.neobliz1.ecomonitoring.platform.analysis.service.TelemetryAnalysisService;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/weather-map")
public class TelemetryAnalysisController {

    private final TelemetryAnalysisService telemetryAnalysisService;

    @GetMapping(value = "/latest", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> getLatestFiveMinuteWeatherMapJson() {
        return telemetryAnalysisService.getLatestFiveMinuteWeatherMapJson()
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }
}