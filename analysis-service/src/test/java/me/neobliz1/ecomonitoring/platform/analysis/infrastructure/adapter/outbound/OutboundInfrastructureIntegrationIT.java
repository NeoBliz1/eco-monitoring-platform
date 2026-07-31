package me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound;

import static me.neobliz1.ecomonitoring.platform.analysis.domain.model.AnalysisConstants.HOT_WINDOW_PREFIX;
import static me.neobliz1.ecomonitoring.platform.analysis.domain.model.AnalysisConstants.WEATHER_HOTWINDOW;
import static me.neobliz1.ecomonitoring.platform.analysis.domain.model.AnalysisConstants.WEATHER_MAP_KEY;
import static me.neobliz1.ecomonitoring.platform.test.common.util.WeatherTestUtils.waitForConsulServicesToBeHealthy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.validator.internal.util.Contracts.assertNotEmpty;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import lombok.extern.slf4j.Slf4j;
import me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.support.IntegrationTestSupport;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.WeatherPacket;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.GridCellLayers;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.WeatherMap;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.ComposeContainer;
import org.testcontainers.junit.jupiter.Container;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

@Slf4j
public class OutboundInfrastructureIntegrationIT extends IntegrationTestSupport {

    @Container
    @SuppressWarnings("unused")
    public static final ComposeContainer ENVIRONMENT = getComposeContainer();

    private static @NotNull String getReadingsAssertMessages(int expectedReadingsNum) {
        return "Total readings across buckets should be "+expectedReadingsNum;
    }

    @BeforeAll
    static void beforeAll() {
        waitForConsulServicesToBeHealthy(List.of(
                "kafka",
                "schema-registry",
                "consul",
                "redis"
        ));
    }

    @Test
    public void shouldProcessCustomAmbientReadings_whenPacketContainsSpecificTemperatureHumidityPressure() throws Exception {
        long currentBucketFloor = getCurrentBucketFloor();
        String stationId = "3";
        double lat = 55.0;
        double lon = -61.0;
        String gridCellKey = calculateGridCellKey(lat, lon);
        WeatherPacket packet = createPacketWithAmbientReadings(
                stationId, currentBucketFloor+1000, lat, lon,
                FLUSH_PACKET_TEMPERATURE, FLUSH_PACKET_HUMIDITY, FLUSH_PACKET_PRESSURE);

        sendPacket(packet);
        sendFlushPackage(stationId, currentBucketFloor, lat, lon);
        WeatherMap map = findWeatherMapByGridCellAnBucketFloor(gridCellKey, currentBucketFloor);

        assertGridCellExists(map, gridCellKey);
        assertAverageTemperature(map, gridCellKey, FLUSH_PACKET_TEMPERATURE, 0.01);
        assertAverageHumidity(map, gridCellKey, FLUSH_PACKET_HUMIDITY, 0.01);
        assertAveragePressure(map, gridCellKey, FLUSH_PACKET_PRESSURE, 0.01);
        assertGridCellReadingCount(map, gridCellKey, 1);
    }

    @Test
    public void shouldProcessCustomAirQualityReadings_whenPacketContainsParticulateMatterVocAndNoise() throws Exception {
        long currentBucketFloor = getCurrentBucketFloor();
        String stationId = "7";
        double lat = 48.8566;
        double lon = 2.3522;
        String gridCellKey = calculateGridCellKey(lat, lon);
        float expectedPm100 = 75.0f;
        float expectedPm25 = 35.5f;
        float expectedPm10 = 50.0f;
        float expectedVocIndex = 2.5f;
        float expectedNoiseDb = 65.0f;
        WeatherPacket packet = createPacketWithAirQualityReadings(
                stationId, currentBucketFloor+1000, lat, lon,
                expectedPm100, expectedPm25, expectedPm10,
                expectedVocIndex, expectedNoiseDb);

        sendPacket(packet);
        sendFlushPackage(stationId, currentBucketFloor, lat, lon);
        WeatherMap map = findWeatherMapByGridCellAnBucketFloor(gridCellKey, currentBucketFloor);

        assertGridCellExists(map, gridCellKey);
        assertAirQualityReadings(map, gridCellKey,
                expectedPm25, expectedPm10, expectedPm100, 0.01);
        assertVocAndNoise(map, gridCellKey, expectedVocIndex, expectedNoiseDb, 0.01);
        assertGridCellReadingCount(map, gridCellKey, 1);
    }

    @Test
    public void shouldProcessCustomPrecipitationReadings_whenPacketContainsRainSnowAndEvaporation() throws Exception {
        long currentBucketFloor = getCurrentBucketFloor();
        String stationId = "12";
        double lat = 40.7128;
        double lon = -74.0060;
        String gridCellKey = calculateGridCellKey(lat, lon);
        float expectedRainRateMmH = 12.5f;
        float expectedSnowDepthCm = 0.0f;
        float expectedEvaporationRate = 2.0f;
        WeatherPacket packet = createPacketWithPrecipitationReadings(
                stationId, currentBucketFloor, lat, lon,
                expectedRainRateMmH, expectedSnowDepthCm, expectedEvaporationRate);

        sendPacket(packet);
        sendFlushPackage(stationId, currentBucketFloor, lat, lon);
        WeatherMap map = findWeatherMapByGridCellAnBucketFloor(gridCellKey, currentBucketFloor);

        assertGridCellExists(map, gridCellKey);
        assertPrecipitationReadings(map, gridCellKey,
                expectedRainRateMmH, expectedSnowDepthCm, expectedEvaporationRate, 0.01);
        assertGridCellReadingCount(map, gridCellKey, 1);
    }

    @Test
    public void shouldProcessCustomOpticalReadings_whenPacketContainsUvSolarLuxAndVisibility() throws Exception {
        long currentBucketFloor = getCurrentBucketFloor();
        String stationId = "22";
        double lat = 35.6762;
        double lon = 139.6503;
        String gridCellKey = calculateGridCellKey(lat, lon);
        float expectedUvIndex = 8.5f;
        float expectedSolarRadiationWm2 = 950.0f;
        float expectedLux = 75000.0f;
        float expectedVisibilityM = 15.0f;
        WeatherPacket packet = createPacketWithOpticalReadings(
                stationId, currentBucketFloor+1000, lat, lon,
                expectedUvIndex, expectedSolarRadiationWm2,
                expectedLux, expectedVisibilityM);

        sendPacket(packet);
        sendFlushPackage(stationId, currentBucketFloor, lat, lon);
        WeatherMap map = findWeatherMapByGridCellAnBucketFloor(gridCellKey, currentBucketFloor);

        assertGridCellExists(map, gridCellKey);
        assertOpticalReadings(map, gridCellKey,
                expectedUvIndex, expectedSolarRadiationWm2,
                expectedLux, expectedVisibilityM, 0.01);
        assertGridCellReadingCount(map, gridCellKey, 1);
    }

    @Test
    public void shouldAggregateDifferentSensorTypes_whenMultiplePacketsWithVariousReadingsSentToSameGridCell() throws Exception {
        long currentBucketFloor = getCurrentBucketFloor();
        String stationId = "25";
        double lat = 55.0;
        double lon = -61.0;
        String gridCellKey = calculateGridCellKey(lat, lon);
        WeatherPacket ambientPacket = createPacketWithAmbientReadings(
                stationId, currentBucketFloor+100, lat, lon, 22.0f, 55.0f, 1013.0f);
        WeatherPacket airQualityPacket = createPacketWithAirQualityReadings(
                stationId, currentBucketFloor+200, lat, lon, 40.0f, 20.0f, 30.0f, 1.5f, 50.0f);
        WeatherPacket precipPacket = createPacketWithPrecipitationReadings(
                stationId, currentBucketFloor+300, lat, lon, 5.0f, 0.0f, 1.5f);
        WeatherPacket opticalPacket = createPacketWithOpticalReadings(
                stationId, currentBucketFloor+400, lat, lon, 6.0f, 700.0f, 50000.0f, 12.0f);

        sendPacket(ambientPacket);
        sendPacket(airQualityPacket);
        sendPacket(precipPacket);
        sendPacket(opticalPacket);
        sendFlushPackage(stationId, currentBucketFloor, lat, lon);
        WeatherMap map = findWeatherMapByGridCellAnBucketFloor(gridCellKey, currentBucketFloor);

        assertGridCellExists(map, gridCellKey);
        assertGridCellReadingCount(map, gridCellKey, 4);
        assertAverageTemperature(map, gridCellKey, 22.0, 0.01);
        assertAverageHumidity(map, gridCellKey, 55.0, 0.01);
        assertAveragePressure(map, gridCellKey, 1013.0, 0.01);
        assertAirQualityReadings(map, gridCellKey, 20.0, 30.0, 40.0, 0.01);
        assertVocAndNoise(map, gridCellKey, 1.5, 50.0, 0.01);
        assertPrecipitationReadings(map, gridCellKey, 5.0, 0.0, 1.5, 0.01);
        assertOpticalReadings(map, gridCellKey, 6.0, 700.0, 50000.0, 12.0, 0.01);
    }

    @Test
    public void shouldHandleExtremeSensorValues_whenPacketsContainBoundaryReadings() throws Exception {
        long currentBucketFloor = getCurrentBucketFloor();
        String stationId = "28";
        double lat = 0.0;
        double lon = 0.0;
        String gridCellKey = calculateGridCellKey(lat, lon);
        WeatherPacket extremeAmbient = createPacketWithAmbientReadings(
                stationId, currentBucketFloor+100, lat, lon,
                -100.0f,  // Minimum valid temperature
                100.0f,   // Maximum valid humidity
                1500.0f); // Maximum valid pressure
        WeatherPacket zeroAirQuality = createPacketWithAirQualityReadings(
                stationId, currentBucketFloor+200, lat, lon,
                0.0f, 0.0f, 0.0f, 0.0f, 0.0f); // All minimum values
        WeatherPacket zeroPrecip = createPacketWithPrecipitationReadings(
                stationId, currentBucketFloor+300, lat, lon,
                0.0f, 0.0f, 0.0f); // No precipitation
        WeatherPacket zeroOptical = createPacketWithOpticalReadings(
                stationId, currentBucketFloor+400, lat, lon,
                0.0f, 0.0f, 0.0f, 0.0f); // Complete darkness

        sendPacket(extremeAmbient);
        sendPacket(zeroAirQuality);
        sendPacket(zeroPrecip);
        sendPacket(zeroOptical);
        sendFlushPackage(stationId, currentBucketFloor, lat, lon);
        WeatherMap map = findWeatherMapByGridCellAnBucketFloor(gridCellKey, currentBucketFloor);

        assertGridCellExists(map, gridCellKey);
        assertGridCellReadingCount(map, gridCellKey, 4);
        assertAverageTemperature(map, gridCellKey, -100.0, 0.01);
        assertAverageHumidity(map, gridCellKey, 100.0, 0.01);
        assertAveragePressure(map, gridCellKey, 1500.0, 0.01);
    }

    @Test
    public void shouldCalculateCorrectAverages_when10PacketsWithKnownValuesSentToSameGridCell() throws Exception {
        long currentBucketFloor = getCurrentBucketFloor();
        String stationId = "20";
        double lat = 55.0;
        double lon = -61.0;
        String gridCellKey = calculateGridCellKey(lat, lon);
        List<WeatherPacket> packets = generateMockPacketsWithKnownValues(
                stationId, currentBucketFloor, lat, lon);

        sendPackets(packets, 50);
        sendFlushPackage(stationId, currentBucketFloor, lat, lon);
        WeatherMap map = findWeatherMapByGridCellAnBucketFloor(gridCellKey, currentBucketFloor);

        assertGridCellExists(map, gridCellKey);
        assertAverageTemperature(map, gridCellKey, 24.5, 0.1);
        assertAverageHumidity(map, gridCellKey, 45.0, 0.1);
        assertAveragePressure(map, gridCellKey, 1013.25, 0.1);
        assertGridCellReadingCount(map, gridCellKey, 10);
    }

    @Test
    public void shouldProcessAllSensorReadingTypes_whenPacketContainsAmbientWindAirQualityPrecipitationOptical() throws Exception {
        long currentBucketFloor = getCurrentBucketFloor();
        String stationId = "5";
        double lat = 55.0;
        double lon = -61.0;
        String gridCellKey = calculateGridCellKey(lat, lon);
        WeatherPacket packet = createFullSensorPacket(stationId, currentBucketFloor, lat, lon);

        sendPacket(packet);
        sendFlushPackage(stationId, currentBucketFloor, lat, lon);
        WeatherMap map = findWeatherMapByGridCellAnBucketFloor(gridCellKey, currentBucketFloor);

        assertGridCellExists(map, gridCellKey);
        assertGridCellHasAllSensorTypes(map, gridCellKey);
    }

    @Test
    public void shouldHandleBucketBoundaryCrossing_whenPacketsSpanTwoAdjacentTimeBuckets() throws Exception {
        long currentBucketFloor = getCurrentBucketFloor();
        long nextBucketFloor = getNextBucketFloor(currentBucketFloor);
        String stationId = "12";
        double lat = 55.0;
        double lon = -61.0;
        String gridCellKey = calculateGridCellKey(lat, lon);
        List<WeatherPacket> firstBatch = new ArrayList<>();
        int expectedReadingsNum = 0;
        for(int i = 0; i<9; i++) {
            firstBatch.add(createBasicPacket(stationId, currentBucketFloor+(i*1000), lat, lon));
            expectedReadingsNum++;
        }
        WeatherPacket secondBatchPacket = createBasicPacket(stationId, nextBucketFloor+100, lat, lon);

        sendPackets(firstBatch, 20);
        sendPacket(secondBatchPacket);
        List<WeatherMap> weatherMaps = collectHistoryRecords(2);
        long totalReadingsMap = getReadingsNum(weatherMaps, gridCellKey, currentBucketFloor);
        long totalReadingsNextMap = getReadingsNum(weatherMaps, gridCellKey, nextBucketFloor);
        List<Long> bucketTimestamps = weatherMaps.stream()
                .map(WeatherMap::getTimestampBucket)
                .toList();

        assertEquals(expectedReadingsNum, totalReadingsMap, getReadingsAssertMessages(expectedReadingsNum));
        assertEquals(1, totalReadingsNextMap, getReadingsAssertMessages(1));
        assertEquals(2, bucketTimestamps.size(), "Total timestamps across buckets should be 2");
    }

    @Test
    public void shouldHandleEdgeCaseGeoCoordinates_whenLatitudeOrLongitudeAreAtBoundaries() throws Exception {
        long currentBucketFloor = getCurrentBucketFloor();
        String stationId = "8";
        double[][] edgeCoords = getEdgeCaseCoordinates();
        int targetRecordsNum = edgeCoords.length;

        for(double[] coord : edgeCoords) {
            double lat = coord[0];
            double lon = coord[1];
            WeatherPacket packet = createBasicPacket(stationId, currentBucketFloor+1000, lat, lon);
            sendPacket(packet);
            sendFlushPackage(stationId, currentBucketFloor, lat, lon);
            currentBucketFloor+=100;
        }

        List<WeatherMap> weatherMaps = collectHistoryRecords(targetRecordsNum);

        assertNotEmpty(weatherMaps, "Should have history records");
        assertEquals(targetRecordsNum, weatherMaps.size() );
        for(WeatherMap map : weatherMaps) {
            map.getGridCellsMap().keySet().forEach(key -> {
                assertNotNull(key, "Grid cell key should not be null");
                assertFalse(key.isEmpty(), "Grid cell key should not be empty");
                assertTrue(key.matches(".*#.*#.*"), "Grid cell key should contain underscore");
            });
        }
    }

    @Test
    public void shouldDropDuplicatePackets_whenSamePacketSentTwiceWithinDedupWindow() throws Exception {
        long currentBucketFloor = getCurrentBucketFloor();
        String stationId = "1";
        double lat = 55.123;
        double lon = -61.345;
        long timestamp = currentBucketFloor+1000;
        String gridCellKey = calculateGridCellKey(lat, lon);
        WeatherPacket packet = createBasicPacket(stationId, timestamp, lat, lon);

        sendPacket(packet);
        Thread.sleep(100);
        sendPacket(packet);
        sendFlushPackage(stationId, currentBucketFloor, lat, lon);
        Optional<WeatherMap> targetMap = collectHistoryRecords(2).stream()
                .filter(m -> m.getTimestampBucket()==currentBucketFloor)
                .findFirst();
        List<WeatherPacket> weatherPackets = collectRawRecords();

        assertTrue(targetMap.isPresent(), "Should have history record for current bucket");
        assertGridCellExists(targetMap.get(), gridCellKey);
        assertGridCellReadingCount(targetMap.get(), gridCellKey, 1);
        assertNotNull(weatherPackets);
        assertEquals(1, weatherPackets.size());
        assertEquals(stationId, weatherPackets.getFirst().getStationId());
    }

    @Test
    public void shouldProcessLoadSpike_when100PacketsAreSentDuring10SecondInterval() throws Exception {
        long currentWindowTimeFloor = getCurrentBucketFloor();
        String stationId = "STATION_INTEGRATION_01";
        double lat = 55.123;
        double lon = -61.345;
        String expectedGridCellFieldKey = calculateGridCellKey(lat, lon);
        String expectedHistoryRedisKey = WEATHER_MAP_KEY+currentWindowTimeFloor;
        String expectedHotWindowRedisKey = WEATHER_HOTWINDOW+expectedGridCellFieldKey;
        List<WeatherPacket> batchPackets = generateMockPackets(100, stationId, currentWindowTimeFloor, lat, lon);

        for (WeatherPacket packet : batchPackets) {
            testProducer.send(new ProducerRecord<>(
                    kafkaIngestionTopic,
                    0,
                    packet.getTimestamp(),
                    packet.getStationId(),
                    packet
            )).get();
        }
        testProducer.flush();
        ConsumerRecord<String, WeatherPacket> rawRecord = pollSingleRecord(rawTopicConsumer);
        long advancedWindowTimeFloor = currentWindowTimeFloor + 300_000L;
        List<WeatherPacket> batch2Packets = generateMockPackets(101, stationId, advancedWindowTimeFloor, lat, lon);
        for (WeatherPacket packet : batch2Packets) {
            testProducer.send(new ProducerRecord<>(kafkaIngestionTopic, 0, packet.getTimestamp(), packet.getStationId(), packet)).get();
        }
        sendFlushPackage(stationId, currentWindowTimeFloor, lat, lon);
        WeatherMap weatherMap = findWeatherMapByGridCellAnBucketFloor(expectedGridCellFieldKey, currentWindowTimeFloor);
        Map<Object, Object> liveRedisHashMatrix = protobufRedisTemplate.opsForHash().entries(expectedHistoryRedisKey);
        Map<Object, Object> liveRedisHotWindowMatrix = redisTemplate.opsForHash().entries(expectedHotWindowRedisKey);

        assertNotNull(rawRecord);
        assertEquals(stationId, rawRecord.value().getStationId());
        assertNotNull(weatherMap);
        assertEquals(currentWindowTimeFloor, weatherMap.getTimestampBucket());
        assertEquals(201, getReadingsNum(List.of(weatherMap), expectedGridCellFieldKey, currentWindowTimeFloor));
        assertTrue(weatherMap.getGridCellsMap().keySet().stream()
                .anyMatch(k -> k.contains(expectedGridCellFieldKey)));
        assertThat(liveRedisHotWindowMatrix).isNotEmpty();
        assertTrue(liveRedisHotWindowMatrix.containsKey(HOT_WINDOW_PREFIX+stationId));
        assertThat(liveRedisHashMatrix).isNotEmpty();
        assertTrue(liveRedisHashMatrix.keySet().stream()
                .anyMatch(k -> {
                    if (k instanceof String sK) {
                        return sK.contains(expectedGridCellFieldKey);
                    }
                    return false;
                })
        );
    }

    @Test
    public void shouldExpireRedisKeysAfter24Hours_whenWeatherDataIsPersisted() throws Exception {
        long currentWindowTimeFloor = getCurrentBucketFloor();
        String stationId = "10";
        double lat = 55.0;
        double lon = -61.0;
        String expectedGridCellFieldKey = calculateGridCellKey(lat, lon);
        String historyKey = WEATHER_MAP_KEY+currentWindowTimeFloor;
        String hotWindowKey = WEATHER_HOTWINDOW+expectedGridCellFieldKey;
        List<WeatherPacket> packets = generateMockPackets(3, stationId, currentWindowTimeFloor, lat, lon);

        sendPackets(packets, 100);
        sendFlushPackage(stationId, currentWindowTimeFloor, lat, lon);
        WeatherMap weatherMap = findWeatherMapByGridCellAnBucketFloor(expectedGridCellFieldKey, currentWindowTimeFloor);
        Long historyTTL = protobufRedisTemplate.getExpire(historyKey, TimeUnit.MINUTES);
        Long hotWindowTTL = protobufRedisTemplate.getExpire(hotWindowKey, TimeUnit.MINUTES);

        assertEquals(currentWindowTimeFloor, weatherMap.getTimestampBucket());
        assertNotNull(historyTTL, "historyTTL should exist");
        assertTrue(historyTTL>1435, "historyTTL should have positive TTL");
        assertTrue(historyTTL<=1440, "historyTTL should not exceed 24 hours");
        assertNotNull(historyTTL, "hotWindowTTL key should exist");
        assertTrue(hotWindowTTL>1435, "hotWindowTTL should have positive TTL");
        assertTrue(hotWindowTTL<=1440, "hotWindowTTL should not exceed 24 hours");
    }

    @Test
    public void shouldCombineDataForMultipleStations_whenStationsSendSimultaneousPackets() throws Exception {
        String[] stationIds = { "11", "15", "30" };
        double lat = 55.0;
        double lon = -61.0;
        String gridCellKey = calculateGridCellKey(lat, lon);
        long currentBucketFloor = getCurrentBucketFloor();

        sendWarmupPackage(currentBucketFloor);
        for(String stationId : stationIds) {
            sendPacket(createBasicPacket(stationId, currentBucketFloor, lat, lon));
        }
        sendFlushPackage("11", currentBucketFloor, lat, lon);
        WeatherMap map = findWeatherMapByGridCellAnBucketFloor(gridCellKey, currentBucketFloor);

        assertGridCellExists(map, gridCellKey);
        assertGridCellReadingCount(map, gridCellKey, 3);
    }

    @Test
    public void shouldAcceptValidStationIdsAtBoundaries_whenStationIdIs1Or30() throws Exception {
        long currentWindowTimeFloor = getCurrentBucketFloor();
        String[] stationIds = { "1", "30" };
        double lat = 55.0;
        double lon = -61.0;
        String gridCellKey = calculateGridCellKey(lat, lon);

        for(String stationId : stationIds) {
            WeatherPacket packet = createBasicPacket(stationId, currentWindowTimeFloor+1000, lat, lon);
            sendPacket(packet);
            Thread.sleep(50);
        }
        sendFlushPackage("1", currentWindowTimeFloor, lat, lon);
        WeatherMap weatherMap = findWeatherMapByGridCellAnBucketFloor(gridCellKey, currentWindowTimeFloor);

        assertGridCellExists(weatherMap, gridCellKey);
        assertGridCellReadingCount(weatherMap, gridCellKey, 2);
    }

    @Test
    public void shouldAverageWindDirectionsUsingVectorMethod_whenPacketsHaveDirectionalWindData() throws Exception {
        long currentWindowTimeFloor = getCurrentBucketFloor();
        String stationId = "25";
        double lat = 55.0;
        double lon = -61.0;
        String gridCellKey = calculateGridCellKey(lat, lon);
        WeatherPacket packet1 = createPacketWithWindReadings(stationId, currentWindowTimeFloor, lat, lon, 10.0f, 350, 15.0f);
        WeatherPacket packet2 = createPacketWithWindReadings(stationId, currentWindowTimeFloor+100, lat, lon, 20.0f, 10, 30.0f);

        sendPacket(packet1);
        sendPacket(packet2);
        sendFlushPackage("1", currentWindowTimeFloor, lat, lon);
        WeatherMap weatherMap = findWeatherMapByGridCellAnBucketFloor(gridCellKey, currentWindowTimeFloor);

        assertGridCellExists(weatherMap, gridCellKey);
        assertVectorWindDirectionAveraging(weatherMap, gridCellKey, 0.0, 1.0);
        assertAverageWindSpeed(weatherMap, gridCellKey, 15.0, 0.1);
    }

    private static long getReadingsNum(List<WeatherMap> weatherMaps, String gridCellKey, long bucketFloor) {
        String gridKey = "0000"+bucketFloor+"#"+gridCellKey;
        return weatherMaps.stream()
                .filter(m -> m.getGridCellsMap().keySet().stream()
                        .anyMatch(k -> k.contains(gridCellKey)
                                && k.contains(String.valueOf(bucketFloor))))
                .map(m -> m.getGridCellsMap().get(gridKey))
                .mapToLong(GridCellLayers::getReadingCount)
                .sum();
    }
}