package me.neobliz1.ecomonitoring.platform.analysis.domain.service;

import static me.neobliz1.ecomonitoring.platform.analysis.domain.model.AnalysisConstants.GRID_BUCKET_KEY_FORMAT;
import static me.neobliz1.ecomonitoring.platform.analysis.domain.model.AnalysisConstants.HOT_WINDOW_PREFIX;
import static me.neobliz1.ecomonitoring.platform.common.constant.PlatformConstants.GEOHASH_SEPARATOR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.withinPercentage;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import me.neobliz1.ecomonitoring.platform.analysis.domain.port.outbound.TelemetryPersistenceRepository;
import me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.messaging.kafka.processor.model.ExtractionMatrix;
import me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.messaging.kafka.record.PortWeatherPacket;
import me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.messaging.kafka.record.WeatherMapRecord;
import me.neobliz1.ecomonitoring.platform.analysis.infrastructure.config.AnalysisInfrastructureProperties;
import me.neobliz1.ecomonitoring.platform.model.exception.ProtocolBufferTranslationException;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.AirQualityReading;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.AmbientReading;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.SensorReading;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.WeatherPacket;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.GridCellLayers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@ExtendWith(MockitoExtension.class)
class TelemetryStatePersisterTest {

    private static final int AGGREGATION_INTERVAL_SECONDS = 60;
    private static final long PACKET_TIMESTAMP = 1700000000000L;
    private static final long SINGLE_BUCKET_TIMESTAMP = 1800000000L;
    private static final double LAT_GRID = 55.123;
    private static final double LON_GRID = 37.456;
    private static final String STATION_ID = "42";
    private static final String SAMPLE_GEOHASH = "55.123#37.456";
    private static final String EXPECTED_STATION_FIELD = HOT_WINDOW_PREFIX+STATION_ID;
    private static final String EXPECTED_GEOHASH_KEY = LAT_GRID+GEOHASH_SEPARATOR+LON_GRID;
    private static final String SAMPLE_SPATIAL_KEY = SINGLE_BUCKET_TIMESTAMP+"#"+SAMPLE_GEOHASH;
    private static final String EXPECTED_TIMESTAMP = String.format(GRID_BUCKET_KEY_FORMAT, PACKET_TIMESTAMP);

    @Mock
    private TelemetryPersistenceRepository telemetryRepository;

    private TelemetryStatePersister persister;

    @BeforeEach
    void setUp() {
        AnalysisInfrastructureProperties props = new AnalysisInfrastructureProperties();
        props.getKafka().getStreams().getPipeline().getName()
                .getAggregationProcessor().setInterval(AGGREGATION_INTERVAL_SECONDS);
        persister = new TelemetryStatePersister(telemetryRepository, props);
    }

    @Test
    void shouldSaveToRealTimeSlidingWindow_whenWeatherPacketIsValid() {
        PortWeatherPacket packet = buildPortWeatherPacket(LAT_GRID, LON_GRID);

        persister.updateRealTimeSlidingWindow(packet);

        verify(telemetryRepository).saveRealTimeSlidingWindow(EXPECTED_GEOHASH_KEY, EXPECTED_STATION_FIELD, EXPECTED_TIMESTAMP);
    }

    @Test
    void shouldSaveToRealTimeSlidingWindow_whenLatGridIsNegative() {
        double negativeLatGrid = -34.567;
        String geohashKey = negativeLatGrid+GEOHASH_SEPARATOR+LON_GRID;

        persister.updateRealTimeSlidingWindow(buildPortWeatherPacket(negativeLatGrid, LON_GRID));

        verify(telemetryRepository).saveRealTimeSlidingWindow(geohashKey, EXPECTED_STATION_FIELD, EXPECTED_TIMESTAMP);
    }

    @Test
    void shouldSaveToRealTimeSlidingWindow_whenLonGridIsNegative() {
        double negativeLonGrid = -120.789;
        String geohashKey = LAT_GRID+GEOHASH_SEPARATOR+negativeLonGrid;

        persister.updateRealTimeSlidingWindow(buildPortWeatherPacket(LAT_GRID, negativeLonGrid));

        verify(telemetryRepository).saveRealTimeSlidingWindow(geohashKey, EXPECTED_STATION_FIELD, EXPECTED_TIMESTAMP);
    }

    @Test
    void shouldReturnEmptyList_whenAggregationHistoryReceivesEmptyMatrix() {
        ExtractionMatrix emptyMatrix = ExtractionMatrix.empty();

        List<WeatherMapRecord> result = persister.processAndComputeAggregatedHistory(emptyMatrix);

        assertThat(result).isEmpty();
    }

    @Test
    void shouldSaveGridCellAndReturnRecord_whenAggregationHistoryReceivesSingleBucket() {
        ExtractionMatrix matrix = buildMatrixWithSingleBucket();

        List<WeatherMapRecord> result = persister.processAndComputeAggregatedHistory(matrix);

        assertThat(result).hasSize(1);
        assertThat(result.getFirst().key()).isEqualTo(SAMPLE_SPATIAL_KEY);
        verify(telemetryRepository).saveHistoricalGridCellLayer(eq(SAMPLE_SPATIAL_KEY), any(byte[].class));
    }

    @Test
    void shouldReturnMultipleRecords_whenAggregationHistoryReceivesMultipleBuckets() {
        long secondBucket = 2000000000L;
        Map<Long, Map<String, List<WeatherPacket>>> container = new HashMap<>();
        container.put(SINGLE_BUCKET_TIMESTAMP, buildSpatialMap(SAMPLE_SPATIAL_KEY));
        container.put(secondBucket, buildSpatialMap(secondBucket+"#"+SAMPLE_GEOHASH));
        ExtractionMatrix matrix = new ExtractionMatrix(container, new ArrayList<>());

        List<WeatherMapRecord> result = persister.processAndComputeAggregatedHistory(matrix);

        assertThat(result).hasSize(2);
    }

    @Test
    void shouldPersistGridCellLayerAllGridCells_whenAggregationHistoryReceivesMultipleSpatialKeys() {
        String secondGeohash = "55.999#37.999";
        String secondSpatialKey = SINGLE_BUCKET_TIMESTAMP+"#"+secondGeohash;
        Map<String, List<WeatherPacket>> spatial = new HashMap<>();
        spatial.put(SAMPLE_SPATIAL_KEY, List.of(buildWeatherPacket()));
        spatial.put(secondSpatialKey, List.of(buildWeatherPacket()));
        Map<Long, Map<String, List<WeatherPacket>>> container = new HashMap<>();
        container.put(SINGLE_BUCKET_TIMESTAMP, spatial);
        ExtractionMatrix matrix = new ExtractionMatrix(container, new ArrayList<>());

        persister.processAndComputeAggregatedHistory(matrix);

        verify(telemetryRepository).saveHistoricalGridCellLayer(eq(SAMPLE_SPATIAL_KEY), any(byte[].class));
        verify(telemetryRepository).saveHistoricalGridCellLayer(eq(secondSpatialKey), any(byte[].class));
    }

    @Test
    void shouldSetIntervalMinutesOnWeatherMap_whenAggregationHistoryCalled() {
        ExtractionMatrix matrix = buildMatrixWithSingleBucket();

        List<WeatherMapRecord> result = persister.processAndComputeAggregatedHistory(matrix);

        assertThat(result.getFirst().payload().getIntervalMinutes()).isEqualTo(1);
    }

    @Test
    void shouldSetTimestampBucketOnWeatherMap_whenAggregationHistoryCalled() {
        Map<Long, Map<String, List<WeatherPacket>>> container = new HashMap<>();
        container.put(SINGLE_BUCKET_TIMESTAMP, buildSpatialMap(SAMPLE_SPATIAL_KEY));
        ExtractionMatrix matrix = new ExtractionMatrix(container, new ArrayList<>());

        List<WeatherMapRecord> result = persister.processAndComputeAggregatedHistory(matrix);

        assertThat(result.getFirst().payload().getTimestampBucket()).isEqualTo(SINGLE_BUCKET_TIMESTAMP);
    }

    @Test
    void shouldSetReadingCountOnGridCellLayers_whenMultiplePacketsPresent() {
        List<WeatherPacket> threePackets = List.of(
                buildWeatherPacket(),
                buildWeatherPacket(),
                buildWeatherPacket()
        );
        Map<String, List<WeatherPacket>> spatial = new HashMap<>();
        spatial.put(SAMPLE_SPATIAL_KEY, threePackets);
        Map<Long, Map<String, List<WeatherPacket>>> container = new HashMap<>();
        container.put(SINGLE_BUCKET_TIMESTAMP, spatial);
        ExtractionMatrix matrix = new ExtractionMatrix(container, new ArrayList<>());

        List<WeatherMapRecord> result = persister.processAndComputeAggregatedHistory(matrix);

        GridCellLayers cellLayers = result.getFirst().payload().getGridCellsMap().get(SAMPLE_GEOHASH);
        assertThat(cellLayers.getReadingCount()).isEqualTo(3);
    }

    @Test
    void shouldAggregateAmbientReadingsIntoGridCellLayers_whenPacketsContainAmbientSensorData() {
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
        spatial.put(SAMPLE_SPATIAL_KEY, List.of(packet));
        Map<Long, Map<String, List<WeatherPacket>>> container = new HashMap<>();
        container.put(SINGLE_BUCKET_TIMESTAMP, spatial);
        ExtractionMatrix matrix = new ExtractionMatrix(container, new ArrayList<>());

        List<WeatherMapRecord> result = persister.processAndComputeAggregatedHistory(matrix);

        GridCellLayers cellLayers = result.getFirst().payload().getGridCellsMap().get(SAMPLE_GEOHASH);
        assertThat(cellLayers.getAvgTemperature()).isCloseTo(expectedTemp, withinPercentage(1));
        assertThat(cellLayers.getAvgHumidity()).isCloseTo(expectedHumidity, withinPercentage(1));
        assertThat(cellLayers.getAvgPressure()).isCloseTo(expectedPressure, withinPercentage(1));
        assertThat(cellLayers.getAvgLeafWetnessPct()).isCloseTo(expectedLeafWetness, withinPercentage(1));
    }

    @Test
    void shouldSetGeohashOnGridCellLayers_whenAggregationMatrixContainsSpatialKey() {
        ExtractionMatrix matrix = buildMatrixWithSingleBucket();

        List<WeatherMapRecord> result = persister.processAndComputeAggregatedHistory(matrix);

        assertThat(result.getFirst().payload().getGridCellsMap()).containsKey(SAMPLE_GEOHASH);
    }

    @Test
    void shouldReturnCorrectWeatherMapRecordKey_whenAggregationMatrixContainsBucket() {
        ExtractionMatrix matrix = buildMatrixWithSingleBucket();

        List<WeatherMapRecord> result = persister.processAndComputeAggregatedHistory(matrix);

        assertThat(result.getFirst().key()).isEqualTo(SAMPLE_SPATIAL_KEY);
    }

    @Test
    void shouldHandleSinglePacketInAggregation_whenSpatialMatrixContainsOnePacket() {
        ExtractionMatrix matrix = buildMatrixWithSingleBucket();

        List<WeatherMapRecord> result = persister.processAndComputeAggregatedHistory(matrix);

        assertThat(result).hasSize(1);
        assertThat(result.getFirst().payload().getGridCellsMap()).isNotEmpty();
    }

    @Test
    void shouldThrowProtocolBufferTranslationException_whenSaveHistoricalGridCellFails() {
        ExtractionMatrix matrix = buildMatrixWithSingleBucket();
        doThrow(new RuntimeException("DB error"))
                .when(telemetryRepository).saveHistoricalGridCellLayer(any(String.class), any(byte[].class));

        assertThatThrownBy(() -> persister.processAndComputeAggregatedHistory(matrix))
                .isInstanceOf(ProtocolBufferTranslationException.class)
                .hasMessageContaining("Domain aggregation encoding sequence failed");
    }

    @Test
    void shouldPersistGridCellLayerEachSpatialKeyOncePerBucket_whenSameSpatialKeyAppearsInTwoBuckets() {
        long secondBucket = 2000000000L;
        Map<Long, Map<String, List<WeatherPacket>>> container = new HashMap<>();
        container.put(SINGLE_BUCKET_TIMESTAMP, buildSpatialMap(SAMPLE_SPATIAL_KEY));
        container.put(secondBucket, buildSpatialMap(SAMPLE_SPATIAL_KEY));
        ExtractionMatrix matrix = new ExtractionMatrix(container, new ArrayList<>());

        persister.processAndComputeAggregatedHistory(matrix);

        verify(telemetryRepository, times(2))
                .saveHistoricalGridCellLayer(eq(SAMPLE_SPATIAL_KEY), any(byte[].class));
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

    private PortWeatherPacket buildPortWeatherPacket(double latGrid, double lonGrid) {
        return new PortWeatherPacket(buildWeatherPacket(), latGrid, lonGrid);
    }

    private Map<String, List<WeatherPacket>> buildSpatialMap(String spatialKey) {
        Map<String, List<WeatherPacket>> spatial = new HashMap<>();
        spatial.put(spatialKey, List.of(buildWeatherPacket()));
        return spatial;
    }

    private ExtractionMatrix buildMatrixWithSingleBucket() {
        Map<Long, Map<String, List<WeatherPacket>>> container = new HashMap<>();
        container.put(SINGLE_BUCKET_TIMESTAMP, buildSpatialMap(SAMPLE_SPATIAL_KEY));
        return new ExtractionMatrix(container, new ArrayList<>());
    }
}