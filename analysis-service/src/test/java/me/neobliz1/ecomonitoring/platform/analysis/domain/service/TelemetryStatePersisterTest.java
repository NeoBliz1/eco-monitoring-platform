package me.neobliz1.ecomonitoring.platform.analysis.domain.service;

import static me.neobliz1.ecomonitoring.platform.analysis.domain.model.AnalysisConstants.GRID_BUCKET_KEY_FORMAT;
import static me.neobliz1.ecomonitoring.platform.analysis.domain.model.AnalysisConstants.HOT_WINDOW_PREFIX;
import static me.neobliz1.ecomonitoring.platform.common.constant.PlatformConstants.HASHTAG_DELIMITER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.withinPercentage;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

import me.neobliz1.ecomonitoring.platform.analysis.domain.port.outbound.TelemetryPersistenceRepository;
import me.neobliz1.ecomonitoring.platform.model.exception.ProtocolBufferTranslationException;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.AirQualityReading;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.AmbientReading;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.SensorReading;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.WeatherPacket;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.GridCellLayers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@ExtendWith(MockitoExtension.class)
class TelemetryStatePersisterTest {

    private static final int AGGREGATION_INTERVAL_SECONDS = 60;
    private static final long PACKET_TIMESTAMP = 1700000000000L;
    private static final String STATION_ID = "42";
    private static final double LAT_GRID = 55.123;
    private static final double LON_GRID = 37.456;
    private static final String EXPECTED_GEOHASH_KEY = LAT_GRID+HASHTAG_DELIMITER+LON_GRID;
    private static final String EXPECTED_STATION_FIELD = HOT_WINDOW_PREFIX+STATION_ID;
    private static final String EXPECTED_TIMESTAMP = String.format(GRID_BUCKET_KEY_FORMAT, PACKET_TIMESTAMP);

    private static final String SAMPLE_GEOSHAH = "55.123#37.456";
    private static final long SINGLE_BUCKET_TIMESTAMP = 1800000000L;
    private static final long MATRIX_BUCKET_TIMESTAMP = 1000000000L;

    @Mock
    private TelemetryPersistenceRepository telemetryRepository;

    @InjectMocks
    private TelemetryStatePersister persister;

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(persister, "aggregationSecondsPerInterval", AGGREGATION_INTERVAL_SECONDS);
    }

    @Test
    void shouldSaveToRealTimeSlidingWindow_whenWeatherPacketIsValid() {
        WeatherPacket packet = buildWeatherPacket();

        persister.updateRealTimeSlidingWindow(packet, LAT_GRID, LON_GRID);

        verify(telemetryRepository).saveRealTimeSlidingWindow(EXPECTED_GEOHASH_KEY, EXPECTED_STATION_FIELD, EXPECTED_TIMESTAMP);
    }

    @Test
    void shouldSaveToRealTimeSlidingWindow_whenLatGridIsNegative() {
        double negativeLatGrid = -34.567;
        String geohashKey = negativeLatGrid+HASHTAG_DELIMITER+LON_GRID;
        WeatherPacket packet = buildWeatherPacket();

        persister.updateRealTimeSlidingWindow(packet, negativeLatGrid, LON_GRID);

        verify(telemetryRepository).saveRealTimeSlidingWindow(geohashKey, EXPECTED_STATION_FIELD, EXPECTED_TIMESTAMP);
    }

    @Test
    void shouldSaveToRealTimeSlidingWindow_whenLonGridIsNegative() {
        double negativeLonGrid = -120.789;
        String geohashKey = LAT_GRID+HASHTAG_DELIMITER+negativeLonGrid;
        WeatherPacket packet = buildWeatherPacket();

        persister.updateRealTimeSlidingWindow(packet, LAT_GRID, negativeLonGrid);

        verify(telemetryRepository).saveRealTimeSlidingWindow(geohashKey, EXPECTED_STATION_FIELD, EXPECTED_TIMESTAMP);
    }


    @Test
    void shouldReturnEmptyList_whenAggregationHistoryReceivesEmptyMap() {
        Map<Long, Map<String, List<WeatherPacket>>> emptyMatrix = new HashMap<>();

        List<TelemetryStatePersister.WeatherMapRecord> result = persister.processAndComputeAggregatedHistory(emptyMatrix);

        assertThat(result).isEmpty();
    }

    @Test
    void shouldSaveGridCellAndReturnRecord_whenAggregationHistoryReceivesSingleBucket() {
        Map<Long, Map<String, List<WeatherPacket>>> matrix = buildMatrixWithSingleBucket();
        String floorBucketKey = String.valueOf(SINGLE_BUCKET_TIMESTAMP);

        List<TelemetryStatePersister.WeatherMapRecord> result = persister.processAndComputeAggregatedHistory(matrix);

        assertThat(result).hasSize(1);
        assertThat(result.getFirst().key()).isEqualTo(floorBucketKey);
        verify(telemetryRepository).saveHistoricalGridCell(eq(SAMPLE_GEOSHAH), any(byte[].class));
    }

    @Test
    void shouldReturnMultipleRecords_whenAggregationHistoryReceivesMultipleBuckets() {
        Map<Long, Map<String, List<WeatherPacket>>> matrix = new HashMap<>();
        long bucket2 = 2000000000L;
        Map<String, List<WeatherPacket>> spatial = new HashMap<>();
        spatial.put(SAMPLE_GEOSHAH, List.of(buildWeatherPacket()));
        matrix.put(MATRIX_BUCKET_TIMESTAMP, spatial);
        matrix.put(bucket2, spatial);

        List<TelemetryStatePersister.WeatherMapRecord> result = persister.processAndComputeAggregatedHistory(matrix);

        assertThat(result).hasSize(2);
    }

    @Test
    void shouldPersistAllGridCells_whenAggregationHistoryReceivesMultipleSpatialKeys() {
        Map<Long, Map<String, List<WeatherPacket>>> matrix = new HashMap<>();
        Map<String, List<WeatherPacket>> spatial = new HashMap<>();
        String secondGeohash = "55.999#37.999";
        spatial.put(SAMPLE_GEOSHAH, List.of(buildWeatherPacket()));
        spatial.put(secondGeohash, List.of(buildWeatherPacket()));
        matrix.put(SINGLE_BUCKET_TIMESTAMP, spatial);

        persister.processAndComputeAggregatedHistory(matrix);

        verify(telemetryRepository).saveHistoricalGridCell(eq(SAMPLE_GEOSHAH), any(byte[].class));
        verify(telemetryRepository).saveHistoricalGridCell(eq(secondGeohash), any(byte[].class));
    }

    @Test
    void shouldPersistCorrectBucketFloorInterval_whenTimestampIsNotAlignedToInterval() {
        Map<Long, Map<String, List<WeatherPacket>>> matrix = new HashMap<>();
        long unalignedTimestamp = 1800000030000L;
        Map<String, List<WeatherPacket>> spatial = new HashMap<>();
        spatial.put(SAMPLE_GEOSHAH, List.of(buildWeatherPacket()));
        matrix.put(unalignedTimestamp, spatial);

        persister.processAndComputeAggregatedHistory(matrix);

        verify(telemetryRepository).saveHistoricalGridCell(eq(SAMPLE_GEOSHAH), any(byte[].class));
    }

    @Test
    void shouldSetIntervalMinutesOnWeatherMap_whenAggregationHistoryCalled() {
        Map<Long, Map<String, List<WeatherPacket>>> matrix = buildMatrixWithSingleBucket();

        List<TelemetryStatePersister.WeatherMapRecord> result = persister.processAndComputeAggregatedHistory(matrix);

        assertThat(result.getFirst().payload().getIntervalMinutes()).isEqualTo(1);
    }

    @Test
    void shouldSetTimestampBucketOnWeatherMap_whenAggregationHistoryCalled() {
        long expectedBucket = 999999999L;
        Map<Long, Map<String, List<WeatherPacket>>> matrix = new HashMap<>();
        Map<String, List<WeatherPacket>> spatial = new HashMap<>();
        spatial.put(SAMPLE_GEOSHAH, List.of(buildWeatherPacket()));
        matrix.put(expectedBucket, spatial);

        List<TelemetryStatePersister.WeatherMapRecord> result = persister.processAndComputeAggregatedHistory(matrix);

        assertThat(result.getFirst().payload().getTimestampBucket()).isEqualTo(expectedBucket);
    }

    @Test
    void shouldSetReadingCountOnGridCellLayers_whenMultiplePacketsPresent() {
        Map<Long, Map<String, List<WeatherPacket>>> matrix = new HashMap<>();
        List<WeatherPacket> threePackets = List.of(
                buildWeatherPacket(),
                buildWeatherPacket(),
                buildWeatherPacket()
        );
        Map<String, List<WeatherPacket>> spatial = new HashMap<>();
        spatial.put(SAMPLE_GEOSHAH, threePackets);
        matrix.put(MATRIX_BUCKET_TIMESTAMP, spatial);

        List<TelemetryStatePersister.WeatherMapRecord> result = persister.processAndComputeAggregatedHistory(matrix);

        GridCellLayers cellLayers = result.getFirst().payload().getGridCellsMap().get(SAMPLE_GEOSHAH);
        assertThat(cellLayers.getReadingCount()).isEqualTo(3);
    }

    @Test
    void shouldAggregateAmbientReadingsIntoGridCellLayers_whenPacketsContainAmbientSensorData() {
        Map<Long, Map<String, List<WeatherPacket>>> matrix = new HashMap<>();
        float expectedTemp = 20.5f;
        float expectedHumidity = 60.0f;
        float expectedPressure = 1013.0f;
        float expectedLeafWetness = 5.0f;
        SensorReading reading = SensorReading.newBuilder()
                .setAmbient(AmbientReading.newBuilder()
                        .setTemperatureC(expectedTemp)
                        .setHumidityPct(expectedHumidity)
                        .setPressureHpa(expectedPressure)
                        .setLeafWetnessPct(expectedLeafWetness)
                        .build())
                .build();

        WeatherPacket packet = WeatherPacket.newBuilder()
                .setStationId(STATION_ID)
                .setTimestamp(PACKET_TIMESTAMP)
                .addReadings(reading)
                .build();

        Map<String, List<WeatherPacket>> spatial = new HashMap<>();
        spatial.put(SAMPLE_GEOSHAH, List.of(packet));
        matrix.put(MATRIX_BUCKET_TIMESTAMP, spatial);

        List<TelemetryStatePersister.WeatherMapRecord> result = persister.processAndComputeAggregatedHistory(matrix);

        GridCellLayers cellLayers = result.getFirst().payload().getGridCellsMap().get(SAMPLE_GEOSHAH);
        assertThat(cellLayers.getAvgTemperature()).isCloseTo(expectedTemp, withinPercentage(1));
        assertThat(cellLayers.getAvgHumidity()).isCloseTo(expectedHumidity, withinPercentage(1));
        assertThat(cellLayers.getAvgPressure()).isCloseTo(expectedPressure, withinPercentage(1));
        assertThat(cellLayers.getAvgLeafWetnessPct()).isCloseTo(expectedLeafWetness, withinPercentage(1));
    }

    @Test
    void shouldSetGeohashOnGridCellLayers_whenAggregationMatrixContainsSpatialKey() {
        Map<Long, Map<String, List<WeatherPacket>>> matrix = buildMatrixWithSingleBucket();

        List<TelemetryStatePersister.WeatherMapRecord> result = persister.processAndComputeAggregatedHistory(matrix);

        assertThat(result.getFirst().payload().getGridCellsMap()).containsKey(SAMPLE_GEOSHAH);
    }

    @Test
    void shouldReturnCorrectWeatherMapRecordKey_whenAggregationMatrixContainsBucket() {
        Map<Long, Map<String, List<WeatherPacket>>> matrix = buildMatrixWithSingleBucket();

        List<TelemetryStatePersister.WeatherMapRecord> result = persister.processAndComputeAggregatedHistory(matrix);

        assertThat(result.getFirst().key()).isEqualTo(String.valueOf(SINGLE_BUCKET_TIMESTAMP));
    }

    @Test
    void shouldHandleSinglePacketInAggregation_whenSpatialMatrixContainsOnePacket() {
        Map<Long, Map<String, List<WeatherPacket>>> matrix = new HashMap<>();
        Map<String, List<WeatherPacket>> spatial = new HashMap<>();
        spatial.put(SAMPLE_GEOSHAH, List.of(buildWeatherPacket()));
        matrix.put(MATRIX_BUCKET_TIMESTAMP, spatial);

        List<TelemetryStatePersister.WeatherMapRecord> result = persister.processAndComputeAggregatedHistory(matrix);

        assertThat(result).hasSize(1);
        assertThat(result.getFirst().payload().getGridCellsMap()).isNotEmpty();
    }

    @Test
    void shouldThrowProtocolBufferTranslationException_whenSaveHistoricalGridCellFails() {
        Map<Long, Map<String, List<WeatherPacket>>> matrix = buildMatrixWithSingleBucket();
        doThrow(new RuntimeException("DB error")).when(telemetryRepository).saveHistoricalGridCell(any(String.class), any(byte[].class));

        assertThatThrownBy(() -> persister.processAndComputeAggregatedHistory(matrix))
                .isInstanceOf(ProtocolBufferTranslationException.class)
                .hasMessageContaining("Domain aggregation encoding sequence failed");
    }

    private WeatherPacket buildWeatherPacket() {
        SensorReading reading = SensorReading.newBuilder()
                .setAmbient(AmbientReading.newBuilder()
                        .setTemperatureC(20.0f)
                        .setHumidityPct(50.0f)
                        .setPressureHpa(1013.25f)
                        .setLeafWetnessPct(0.0f)
                        .build())
                .setAirQuality(AirQualityReading.newBuilder()
                        .setPm100(50.0f)
                        .setPm25(25.0f)
                        .setPm10(30.0f)
                        .setVocIndex(100.0f)
                        .setNoiseDb(60.0f)
                        .build())
                .build();

        return WeatherPacket.newBuilder()
                .setStationId(STATION_ID)
                .setTimestamp(PACKET_TIMESTAMP)
                .addReadings(reading)
                .build();
    }

    private Map<Long, Map<String, List<WeatherPacket>>> buildMatrixWithSingleBucket() {
        Map<Long, Map<String, List<WeatherPacket>>> matrix = new HashMap<>();
        Map<String, List<WeatherPacket>> spatial = new HashMap<>();
        spatial.put(SAMPLE_GEOSHAH, List.of(buildWeatherPacket()));
        matrix.put(SINGLE_BUCKET_TIMESTAMP, spatial);
        return matrix;
    }
}
