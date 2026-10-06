package me.neobliz1.ecomonitoring.platform.analysis.domain.service;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import me.neobliz1.ecomonitoring.platform.analysis.domain.model.dto.WeatherMapAnalysisRequestQuery;
import me.neobliz1.ecomonitoring.platform.analysis.domain.port.inbound.TelemetryQueryService;
import me.neobliz1.ecomonitoring.platform.analysis.domain.port.outbound.TelemetryQueryRepository;
import me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.messaging.kafka.record.WeatherMapRecord;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.WeatherMap;

@Slf4j
@RequiredArgsConstructor
public class TelemetryStateQueryResolver implements TelemetryQueryService {

    private final TelemetryQueryRepository telemetryQueryRepositoryAdapter;

    @Override
    public @NonNull WeatherMapRecord getLatestTimeIntervalWeatherMapByCoordinates(WeatherMapAnalysisRequestQuery request) {
        String spatialKey = String.valueOf(request.targetTimestamp()+request.minLat()+request.maxLat()+request.minLon()+request.maxLon());
        WeatherMap weatherMapByTimestampAndSpatialBox = telemetryQueryRepositoryAdapter.getWeatherMapByTimestampAndSpatialBox(request);
        return new WeatherMapRecord(spatialKey, weatherMapByTimestampAndSpatialBox);
    }
}