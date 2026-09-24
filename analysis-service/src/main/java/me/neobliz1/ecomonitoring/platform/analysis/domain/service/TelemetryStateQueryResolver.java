package me.neobliz1.ecomonitoring.platform.analysis.domain.service;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import me.neobliz1.ecomonitoring.platform.analysis.domain.port.inbound.TelemetryQueryService;
import me.neobliz1.ecomonitoring.platform.analysis.domain.port.outbound.TelemetryQueryRepository;
import me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.messaging.kafka.record.WeatherMapRecord;
import me.neobliz1.ecomonitoring.platform.model.exception.WeatherMapDataNotFoundException;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.WeatherMap;

@Slf4j
@RequiredArgsConstructor
public class TelemetryStateQueryResolver implements TelemetryQueryService {

    private final TelemetryQueryRepository telemetryQueryRepositoryAdapter;
    private final int aggregationSecondsPerInterval;

    @Override
    public @NonNull WeatherMapRecord getLatestTimeIntervalWeatherMapByCoordinates(
            long targetTimestamp, Double minLat, Double maxLat, Double minLon, Double maxLon) {
        if(minLat>maxLat) {
            throw new WeatherMapDataNotFoundException();
        }
        if(minLon>maxLon) {
            throw new WeatherMapDataNotFoundException();
        }
        long activeBucketFloor = TelemetryUtils.getAggregationBucketFloorInterval(targetTimestamp, aggregationSecondsPerInterval);
        double currentMinLat = minLat;
        double currentMaxLat = maxLat;
        double currentMinLon = minLon;
        double currentMaxLon = maxLon;
        double[] expanded = new double[4];
        for(int i = 0; i<4; i++) {
            try {
                WeatherMap weatherMap = telemetryQueryRepositoryAdapter.getWeatherMapByTimestampAndSpatialBox(
                        activeBucketFloor, currentMinLat, currentMaxLat, currentMinLon, currentMaxLon);
                if(weatherMap.getGridCellsMap().isEmpty()) {
                    throw new WeatherMapDataNotFoundException();
                }
                return new WeatherMapRecord(String.valueOf(activeBucketFloor+currentMinLat+currentMaxLat+currentMaxLon+currentMaxLat),
                        weatherMap);
            } catch(WeatherMapDataNotFoundException e) {
                if(log.isDebugEnabled()) {
                    log.debug("No data found for bucket {} at spatial precision tier {}. Expand spatial box...", activeBucketFloor, i);
                }
            }
            expandSpatialBox(minLat, maxLat, minLon, maxLon, i, expanded);
            currentMinLat = expanded[0];
            currentMaxLat = expanded[1];
            currentMinLon = expanded[2];
            currentMaxLon = expanded[3];
        }
        throw new WeatherMapDataNotFoundException();
    }

    private void expandSpatialBox(double minLat, double maxLat, double minLon, double maxLon, int tier, double[] result) {
        int exponent = Math.max(0, 2-tier);
        double scale = Math.pow(10, exponent);
        double latAnchor = Math.round((minLat+maxLat)/2.0d*scale)/scale;
        double lonAnchor = Math.round((minLon+maxLon)/2.0d*scale)/scale;
        double halfSpan = 1.0d/scale;
        result[0] = latAnchor-halfSpan;
        result[1] = latAnchor+halfSpan;
        result[2] = Math.min(lonAnchor-halfSpan, lonAnchor+halfSpan);
        result[3] = Math.max(lonAnchor-halfSpan, lonAnchor+halfSpan);
    }
}