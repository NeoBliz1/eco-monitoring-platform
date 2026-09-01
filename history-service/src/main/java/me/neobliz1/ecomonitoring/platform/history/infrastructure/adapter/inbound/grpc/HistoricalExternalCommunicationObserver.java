package me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter.inbound.grpc;

import io.github.neobliz1.validproto.annotation.ValidProto;
import io.github.neobliz1.validproto.annotation.ValidatedProto;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import me.neobliz1.ecomonitoring.platform.history.domain.model.entity.WeatherGridCellMetric;
import me.neobliz1.ecomonitoring.platform.history.domain.model.entity.WeatherMapBucket;
import me.neobliz1.ecomonitoring.platform.history.domain.port.inbound.HistoricalDataConvertService;
import me.neobliz1.ecomonitoring.platform.history.domain.port.outbound.HistoricalQueryRepository;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.GridCellLayers;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.WeatherMap;
import weather.history.HistoryServiceGrpc;
import weather.history.SpatialBoxRequest;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@Slf4j
@ValidatedProto
@RequiredArgsConstructor
public class HistoricalExternalCommunicationObserver extends HistoryServiceGrpc.HistoryServiceImplBase {

    private final HistoricalQueryRepository historicalQueryRepository;
    private final HistoricalDataConvertService weatherMapConverter;

    @Override
    public void findFilteredGridDataBySpatialBox(@ValidProto SpatialBoxRequest request, StreamObserver<WeatherMap> responseObserver) {
        long timestampBucket = request.getTimestampBucket();
        double minLat = request.getMinLat();
        double maxLat = request.getMaxLat();
        double minLon = request.getMinLon();
        double maxLon = request.getMaxLon();
        int timeIntervalInMinutes = request.getTimeIntervalInMinutes();

        if(log.isDebugEnabled()) {
            log.debug("GRPC request: {} [{}#{}#{}#{}] has been received and is processing.",
                    timestampBucket, minLat, maxLat, minLon, maxLon);
        }

        try {
            WeatherMapBucket weatherMapBucket = historicalQueryRepository
                    .findByTimestampBucketAndIntervalMinutes(timestampBucket, timeIntervalInMinutes)
                    .orElseThrow(() -> Status.NOT_FOUND
                            .withDescription("Weather map bucket not found for given timestamp and interval")
                            .asRuntimeException());
            UUID bucketId = weatherMapBucket.getId();
            List<WeatherGridCellMetric> weatherGridCells = historicalQueryRepository
                    .findByBucketIdAndSpatialBox(bucketId, minLat, maxLat, minLon, maxLon);
            if(weatherGridCells.isEmpty()) {
                throw Status.NOT_FOUND
                        .withDescription("No grid metrics found within the specified spatial box")
                        .asRuntimeException();
            }
            Map<String, GridCellLayers> cellLayersMap = weatherGridCells.stream()
                    .collect(Collectors.toMap(
                            WeatherGridCellMetric::getGeohash,
                            weatherMapConverter::convertWeatherGridCellsToWeatherMap
                    ));
            WeatherMap weatherMap = WeatherMap.newBuilder()
                    .setIntervalMinutes(timeIntervalInMinutes)
                    .setTimestampBucket(timestampBucket)
                    .putAllGridCells(cellLayersMap)
                    .build();
            responseObserver.onNext(weatherMap);
            responseObserver.onCompleted();
        } catch(io.grpc.StatusRuntimeException e) {
            log.warn("Spatial box lookup failed with explicit status: {}", e.getStatus());
            responseObserver.onError(e);
        } catch(Exception e) {
            log.error("Unexpected error processing spatial box request", e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription("An unexpected internal error occurred processing data.")
                    .withCause(e)
                    .asRuntimeException());
        }
    }
}