package me.neobliz1.ecomonitoring.platform.analysis.infrastructure.persistence;

import static me.neobliz1.ecomonitoring.platform.common.constant.PlatformConstants.HASHTAG_DELIMITER;

import lombok.RequiredArgsConstructor;
import me.neobliz1.ecomonitoring.platform.analysis.domain.model.AnalysisConstants;
import me.neobliz1.ecomonitoring.platform.analysis.domain.service.TelemetryAnalysisAccumulator;
import me.neobliz1.ecomonitoring.platform.analysis.domain.service.TelemetryPersistentService;
import me.neobliz1.ecomonitoring.platform.analysis.domain.service.TelemetryUtils;
import me.neobliz1.ecomonitoring.platform.model.exception.ProtocolBufferTranslationException;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.SensorReading;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.WeatherPacket;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.GridCellLayers;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.WeatherMap;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.time.Duration;
import java.util.List;
import java.util.Map;

@RequiredArgsConstructor
public class TelemetryStatePersister implements TelemetryPersistentService {

    private final RedisTemplate<String, byte[]> protobufRedisTemplate;
    private final StringRedisTemplate redisTemplate;

    @Value("${spring.kafka.streams.pipeline.name.aggregation-processor.interval}")
    private Integer aggregationSecondsPerInterval;
    @Value("${spring.redis.records.ttl}")
    private Long redisCacheTtlInterval;

    @Override
    public void updateRealTimeSlidingWindow(WeatherPacket packet, double latGrid, double lonGrid) {
        String redisKey = AnalysisConstants.WEATHER_HOTWINDOW+latGrid+HASHTAG_DELIMITER+lonGrid;
        String hashField = AnalysisConstants.HOT_WINDOW_PREFIX+packet.getStationId();
        redisTemplate.opsForHash().put(redisKey, hashField, String.format(AnalysisConstants.UTC_TIMESTAMP_FORMAT, packet.getTimestamp()));
        redisTemplate.expire(redisKey, Duration.ofHours(redisCacheTtlInterval));
    }

    @Override
    public void persistAggregatedHistory(Map<Long, Map<String, List<WeatherPacket>>> extractionMatrix,
                                         ProcessorContext<String, WeatherMap> context,
                                         long currentStreamTime) {
        extractionMatrix.forEach((bucketTime, spatialMap) -> {
            WeatherMap.Builder weatherMapBuilder = WeatherMap.newBuilder()
                    .setTimestampBucket(bucketTime)
                    .setIntervalMinutes((int) Duration.ofSeconds(aggregationSecondsPerInterval).toMinutes());

            spatialMap.forEach((spatialKey, packetsList) -> {
                GridCellLayers.Builder cellBuilder = aggregatePackets(packetsList);
                cellBuilder.setGeohash(spatialKey);
                weatherMapBuilder.putGridCells(spatialKey, cellBuilder.build());
            });
            String transactionRoutingKey = String.valueOf(bucketTime);
            String redisHistoryKey = AnalysisConstants.WEATHER_MAP_KEY+TelemetryUtils.getAggregationBucketFloorInterval(bucketTime,
                    aggregationSecondsPerInterval);
            WeatherMap finalReport = weatherMapBuilder.build();
            finalReport.getGridCellsMap().forEach((geohash, cellLayers) -> {
                try {
                    byte[] binaryPayload = cellLayers.toByteArray();
                    protobufRedisTemplate.opsForHash().put(redisHistoryKey, geohash, binaryPayload);
                } catch(Exception e) {
                    throw new ProtocolBufferTranslationException(String.format("Failed to serialize sub-grid metrics for geohash: %s", geohash), e);
                }
            });
            protobufRedisTemplate.expire(redisHistoryKey, Duration.ofHours(redisCacheTtlInterval));
            org.apache.kafka.streams.processor.api.Record<String, WeatherMap> record = new Record<>(transactionRoutingKey, finalReport, currentStreamTime);
            context.forward(record);
        });
    }

    private GridCellLayers.Builder aggregatePackets(List<WeatherPacket> packets) {
        TelemetryAnalysisAccumulator resultContainer = packets.parallelStream()
                .collect(
                        TelemetryAnalysisAccumulator::new,
                        (container, packet) -> {
                            for(SensorReading reading : packet.getReadingsList()) {
                                container.accumulate(reading);
                            }
                        },
                        TelemetryAnalysisAccumulator::merge
                );

        return resultContainer.applyTo(GridCellLayers.newBuilder()
                .setReadingCount(packets.size()));
    }
}
