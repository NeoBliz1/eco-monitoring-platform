package me.neobliz1.ecomonitoring.platform.analysis.service;

import static me.neobliz1.ecomonitoring.platform.analysis.service.impl.TelemetryAnalysisServiceImpl.WEATHER_MAP_KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import lombok.extern.slf4j.Slf4j;
import me.neobliz1.ecomonitoring.platform.analysis.BaseKafkaIntegrationTest;
import me.neobliz1.ecomonitoring.platform.analysis.service.impl.TelemetryAnalysisServiceImpl;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.WeatherPacket;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.WeatherMap;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.ComposeContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.File;
import java.time.Instant;
import java.util.List;
import java.util.Map;

@Slf4j
@Testcontainers
public class TelemetryAnalysisServiceIntegrationTest extends BaseKafkaIntegrationTest {

    @Autowired
    private TelemetryAnalysisServiceImpl telemetryAnalysisService;

    @Autowired
    private StringRedisTemplate redisTemplate;

    private String expectedHistoryRedisKey;
    private final String expectedHotWindowRedisKey = "weather:hotwindow:STATION_INTEGRATION_01";

    @Container
    @SuppressWarnings("unused")
    public static ComposeContainer ENVIRONMENT = new ComposeContainer(new File("../docker/analysis-test-docker-compose.yaml"))
            .withEnv(loadEnvironmentMap())
            .withTailChildContainers(true);

    @DynamicPropertySource
    static void overrideClusterProperties(DynamicPropertyRegistry registry) {
        // Statically mapped directly to localhost based on your compose file port blocks
        String broker1 = "localhost:9094";
        String broker2 = "localhost:9194";
        String broker3 = "localhost:9294";

        String redisHost = "localhost";
        int redisPort = 6379;

        String schemaRegUrl = "http://localhost:8085";

        String consulHost = "localhost";
        int consulPort = 8500;

        // Inject configuration mappings into Spring environment
        registry.add("spring.kafka.bootstrap-servers", () -> String.join(",", broker1, broker2, broker3));
        registry.add("spring.data.redis.host", () -> redisHost);
        registry.add("spring.data.redis.port", () -> redisPort);
        registry.add("spring.kafka.properties.schema.registry.url", () -> schemaRegUrl);
        registry.add("spring.cloud.consul.host", () -> consulHost);
        registry.add("spring.cloud.consul.port", () -> consulPort);
    }


    @Test
    public void shouldProcessLoadSpike_when100PacketsAreSentDuring10SecondInterval() throws Exception {
        long currentWindowTimeFloor = Instant.now().toEpochMilli();
        currentWindowTimeFloor = currentWindowTimeFloor-(currentWindowTimeFloor%300_000L);
        expectedHistoryRedisKey = WEATHER_MAP_KEY+currentWindowTimeFloor;
        redisTemplate.delete(expectedHistoryRedisKey);
        redisTemplate.delete(expectedHotWindowRedisKey);
        double inputLatitude = 55.123;
        double inputLongitude = -61.345;
        String stationId = "STATION_INTEGRATION_01";
        String expectedGridCellFieldKey = String.format("%.1f_%.1f",
                Math.round(inputLatitude*10.0)/10.0,
                Math.round(inputLongitude*10.0)/10.0);
        List<WeatherPacket> batchPackets = generateMockPackets(100, stationId, currentWindowTimeFloor, inputLatitude, inputLongitude);

        // Act: Distribute 100 packets over a 10-second interval (10,000ms / 100 packets = 100ms delay per packet)
        long delayPerPacketMs = 100L;
        for(WeatherPacket packet : batchPackets) {
            testProducer.send(new ProducerRecord<>(
                    kafkaIngestionTopic,
                    0,                        // <-- Partition 0 explicitly
                    packet.getTimestamp(),    // Record event timestamp
                    packet.getStationId(),    // Kafka message key
                    packet                    // Message value object
            )).get();

            Thread.sleep(delayPerPacketMs);
        }
        testProducer.flush();
        var rawRecord = pollSingleRecord(rawTopicConsumer);

        long advancedWindowTimeFloor = currentWindowTimeFloor+300_000L; // Advanced exactly 5 minutes out
        List<WeatherPacket> batch2Packets = generateMockPackets(101, stationId, advancedWindowTimeFloor, inputLatitude, inputLongitude);
        for(WeatherPacket packet : batch2Packets) {
            testProducer.send(new ProducerRecord<>(kafkaIngestionTopic, 0, packet.getTimestamp(), packet.getStationId(), packet)).get();
            Thread.sleep(delayPerPacketMs);
        }
        testProducer.flush();
        var historyRecord = pollSingleRecord(historyTopicConsumer);
        WeatherMap completedMapReport = historyRecord.value();
        Map<Object, Object> liveRedisHashMatrix = redisTemplate.opsForHash().entries(expectedHistoryRedisKey);

        assertNotNull(rawRecord);
        assertEquals(stationId, rawRecord.value().getStationId());
        assertNotNull(historyRecord);
        assertThat(completedMapReport.getTimestampBucket())
                .isIn(currentWindowTimeFloor, advancedWindowTimeFloor);
        assertTrue(completedMapReport.getGridCellsMap().keySet().stream().anyMatch(k -> k.contains(expectedGridCellFieldKey)));
        assertThat(liveRedisHashMatrix).isNotEmpty();
        assertTrue(liveRedisHashMatrix.keySet().stream()
                .anyMatch(k -> {
                    if(k instanceof String sK) {
                        return sK.contains(expectedGridCellFieldKey);
                    } else return false;
                })
        );
    }
}