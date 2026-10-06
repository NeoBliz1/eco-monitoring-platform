package me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.history.grpc;

import static me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.messaging.kafka.processor.util.AggregationUtils.getAggregationSecondsPerInterval;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import me.neobliz1.ecomonitoring.platform.analysis.domain.model.dto.WeatherMapAnalysisRequestQuery;
import me.neobliz1.ecomonitoring.platform.analysis.domain.port.outbound.TelemetryQueryArchive;
import me.neobliz1.ecomonitoring.platform.analysis.infrastructure.config.AnalysisInfrastructureProperties;
import me.neobliz1.ecomonitoring.platform.model.exception.WeatherMapDataNotFoundException;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.WeatherMap;
import org.jspecify.annotations.NonNull;
import weather.history.HistoryServiceGrpc;
import weather.history.SpatialBoxRequest;

import java.time.Duration;

@Slf4j
@RequiredArgsConstructor
public class TelemetryQueryGrpcAdapter implements TelemetryQueryArchive {

    private final HistoryServiceGrpc.HistoryServiceBlockingStub historyServiceStub;
    private final AnalysisInfrastructureProperties props;

    @Override
    public @NonNull WeatherMap findGridDataBySpatialBoxInHistoryService(@NonNull WeatherMapAnalysisRequestQuery request) {
        Double minLat = request.minLat();
        Double maxLat = request.maxLat();
        Double minLon = request.minLon();
        Double maxLon = request.maxLon();
        if(log.isDebugEnabled()) {
            log.debug("Querying history-service for spatial box [{}#{}#{}#{}]", minLat, maxLat, minLon, maxLon);
        }
        SpatialBoxRequest spatialBoxRequest = SpatialBoxRequest.newBuilder()
                .setTimestampBucket(request.targetTimestamp())
                .setTimeIntervalInMinutes((int) Duration.ofSeconds(getAggregationSecondsPerInterval(props)).toMinutes())
                .setMinLat(minLat)
                .setMaxLat(maxLat)
                .setMinLon(minLon)
                .setMaxLon(maxLon)
                .build();
        return getWeatherMap(spatialBoxRequest);
    }

    private @NonNull WeatherMap getWeatherMap(@NonNull SpatialBoxRequest spatialBoxRequest) {
        try {
            WeatherMap weatherMap = historyServiceStub.findFilteredGridDataBySpatialBox(spatialBoxRequest);
            if(weatherMap==null || weatherMap.getGridCellsMap().isEmpty()) {
                throw new WeatherMapDataNotFoundException();
            }
            return weatherMap;
        } catch(Exception e) {
            throw new WeatherMapDataNotFoundException();
        }
    }
}