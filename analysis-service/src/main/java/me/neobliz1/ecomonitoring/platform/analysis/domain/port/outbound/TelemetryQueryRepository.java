package me.neobliz1.ecomonitoring.platform.analysis.domain.port.outbound;

import me.neobliz1.ecomonitoring.platform.analysis.domain.model.dto.WeatherMapAnalysisRequestQuery;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.WeatherMap;

public interface TelemetryQueryRepository {

    WeatherMap getWeatherMapByTimestampAndSpatialBox(WeatherMapAnalysisRequestQuery request);
}
