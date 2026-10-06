package me.neobliz1.ecomonitoring.platform.analysis.domain.port.inbound;


import me.neobliz1.ecomonitoring.platform.analysis.domain.model.dto.WeatherMapAnalysisRequestQuery;
import me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.messaging.kafka.record.WeatherMapRecord;

public interface TelemetryQueryService {

    WeatherMapRecord getLatestTimeIntervalWeatherMapByCoordinates(WeatherMapAnalysisRequestQuery request);
}
