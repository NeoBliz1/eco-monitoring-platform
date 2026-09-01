package me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.history.grpc;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import me.neobliz1.ecomonitoring.platform.analysis.domain.port.outbound.TelemetryQueryArchive;
import me.neobliz1.ecomonitoring.platform.model.exception.WeatherMapDataNotFoundException;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.WeatherMap;
import org.springframework.beans.factory.annotation.Value;
import weather.history.HistoryServiceGrpc;
import weather.history.SpatialBoxRequest;

import java.time.Duration;

@Slf4j
@RequiredArgsConstructor
public class TelemetryQueryGrpcAdapter implements TelemetryQueryArchive {

    private final HistoryServiceGrpc.HistoryServiceBlockingStub historyServiceStub;
    @Value("${spring.kafka.streams.pipeline.name.aggregation-processor.interval}")
    Integer interval;

    @Override
    public @NonNull WeatherMap findFilteredGridDataBySpatialBoxInArchive(long activeBucketFloor, double minLat, double maxLat, double minLon, double maxLon) {
        log.info("Executing microservice cross-call to history-service cluster node discovered via Consul");

        SpatialBoxRequest request = SpatialBoxRequest.newBuilder()
                .setTimestampBucket(activeBucketFloor)
                .setTimeIntervalInMinutes((int) Duration.ofMillis(interval).toMinutes())
                .setMinLat(minLat)
                .setMaxLat(maxLat)
                .setMinLon(minLon)
                .setMaxLon(maxLon)
                .build();

        try {
            WeatherMap weatherMap = historyServiceStub.findFilteredGridDataBySpatialBox(request);
            if(weatherMap.getGridCellsMap().isEmpty()) throw new WeatherMapDataNotFoundException();
            return weatherMap;
        } catch(Exception e) {
            throw new WeatherMapDataNotFoundException();
        }
    }
}