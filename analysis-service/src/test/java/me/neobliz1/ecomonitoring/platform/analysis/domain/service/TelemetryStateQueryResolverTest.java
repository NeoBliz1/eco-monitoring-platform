package me.neobliz1.ecomonitoring.platform.analysis.domain.service;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.when;

import com.google.protobuf.InvalidProtocolBufferException;
import me.neobliz1.ecomonitoring.platform.analysis.domain.port.outbound.TelemetryQueryRepository;
import me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.messaging.kafka.record.WeatherMapRecord;
import me.neobliz1.ecomonitoring.platform.model.exception.ProtocolBufferTranslationException;
import me.neobliz1.ecomonitoring.platform.model.exception.WeatherMapDataNotFoundException;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.GridCellLayers;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.WeatherMap;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.HashMap;
import java.util.Map;

@ExtendWith(MockitoExtension.class)
class TelemetryStateQueryResolverTest {

    private static final String VALID_CELL_KEY = "cell#55.5#37.5";
    private static final long TARGET_TIMESTAMP = 1710000000000L;
    private static final double MIN_LAT = 55.0;
    private static final double MAX_LAT = 56.0;
    private static final double MIN_LON = 37.0;
    private static final double MAX_LON = 38.0;

    @Mock
    private TelemetryQueryRepository telemetryQueryRepositoryAdapter;

    private TelemetryStateQueryResolver resolver;

    @BeforeEach
    void setUp() {
        resolver = new TelemetryStateQueryResolver(telemetryQueryRepositoryAdapter, 60);
    }

    @Test
    void shouldReturnJsonWeatherMap_whenCoordinatesAreValidAndDataExists() {
        Map<String, GridCellLayers> rawData = new HashMap<>();
        GridCellLayers layers = GridCellLayers.newBuilder().setAvgTemperature(25.5).build();
        rawData.put(VALID_CELL_KEY, layers);
        when(telemetryQueryRepositoryAdapter.getWeatherMapByTimestampAndSpatialBox(
                anyLong(), anyDouble(), anyDouble(), anyDouble(), anyDouble()))
                .thenReturn(WeatherMap.newBuilder().putAllGridCells(rawData).build());

        WeatherMapRecord mapByCoordinates = resolver.getLatestTimeIntervalWeatherMapByCoordinates(
                TARGET_TIMESTAMP, MIN_LAT, MAX_LAT, MIN_LON, MAX_LON);

        assertTrue(mapByCoordinates.payload().containsGridCells(VALID_CELL_KEY));
    }

    @Test
    void shouldFilterOutCellsOutsideCoordinatesSquare_whenDataIsProcessed() {
        Map<String, GridCellLayers> rawData = new HashMap<>();
        GridCellLayers validLayers = GridCellLayers.newBuilder().setAvgTemperature(25.5).build();
        rawData.put(VALID_CELL_KEY, validLayers);
        when(telemetryQueryRepositoryAdapter.getWeatherMapByTimestampAndSpatialBox(
                anyLong(), anyDouble(), anyDouble(), anyDouble(), anyDouble()))
                .thenReturn(WeatherMap.newBuilder().putAllGridCells(rawData).build());

        WeatherMapRecord mapByCoordinates = resolver.getLatestTimeIntervalWeatherMapByCoordinates(
                TARGET_TIMESTAMP, MIN_LAT, MAX_LAT, MIN_LON, MAX_LON);
        WeatherMap payload = mapByCoordinates.payload();

        assertTrue(payload.containsGridCells(VALID_CELL_KEY));
        assertFalse(payload.containsGridCells("cell#60.0#40.0"));
    }

    @Test
    void shouldThrowWeatherMapDataNotFoundException_whenMinLatExceedsMaxLat() {
        ThrowingCallable executable = () -> resolver.getLatestTimeIntervalWeatherMapByCoordinates(
                TARGET_TIMESTAMP, MAX_LAT, MIN_LAT, MIN_LON, MAX_LON);

        assertThatThrownBy(executable)
                .isInstanceOf(WeatherMapDataNotFoundException.class);
    }

    @Test
    void shouldThrowWeatherMapDataNotFoundException_whenMinLonExceedsMaxLon() {
        ThrowingCallable executable = () -> resolver.getLatestTimeIntervalWeatherMapByCoordinates(
                TARGET_TIMESTAMP, MIN_LAT, MAX_LAT, MAX_LON, MIN_LON);

        assertThatThrownBy(executable)
                .isInstanceOf(WeatherMapDataNotFoundException.class);
    }

    @Test
    void shouldThrowProtocolBufferTranslationException_whenGridValueBytesAreCorrupted() {
        when(telemetryQueryRepositoryAdapter.getWeatherMapByTimestampAndSpatialBox(
                anyLong(), anyDouble(), anyDouble(), anyDouble(), anyDouble()))
                .thenThrow(new ProtocolBufferTranslationException(
                        "Corrupted Protobuf payload for grid cell: "+VALID_CELL_KEY,
                        new InvalidProtocolBufferException("Contents do not match protocol")));

        ThrowingCallable executable = () -> resolver.getLatestTimeIntervalWeatherMapByCoordinates(
                TARGET_TIMESTAMP, MIN_LAT, MAX_LAT, MIN_LON, MAX_LON);

        assertThatThrownBy(executable)
                .isInstanceOf(ProtocolBufferTranslationException.class)
                .hasMessageContaining("Corrupted Protobuf payload for grid cell")
                .hasCauseInstanceOf(InvalidProtocolBufferException.class);
    }

    @Test
    void shouldThrowWeatherMapDataNotFoundException_whenRepositoryReturnsEmptyMatrixMap() {
        when(telemetryQueryRepositoryAdapter.getWeatherMapByTimestampAndSpatialBox(
                anyLong(), anyDouble(), anyDouble(), anyDouble(), anyDouble()))
                .thenThrow(new WeatherMapDataNotFoundException());

        ThrowingCallable executable = () -> resolver.getLatestTimeIntervalWeatherMapByCoordinates(
                TARGET_TIMESTAMP, MIN_LAT, MAX_LAT, MIN_LON, MAX_LON);

        assertThatThrownBy(executable)
                .isInstanceOf(WeatherMapDataNotFoundException.class);
    }
}