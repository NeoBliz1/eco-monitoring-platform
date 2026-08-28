package me.neobliz1.ecomonitoring.platform.analysis.domain.service;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.when;

import com.google.protobuf.InvalidProtocolBufferException;
import me.neobliz1.ecomonitoring.platform.analysis.domain.port.outbound.TelemetryQueryRepository;
import me.neobliz1.ecomonitoring.platform.model.exception.ProtocolBufferTranslationException;
import me.neobliz1.ecomonitoring.platform.model.exception.WeatherMapDataNotFoundException;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.GridCellLayers;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.WeatherMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@ExtendWith(MockitoExtension.class)
class TelemetryStateQueryResolverTest {

    private static final int AGGREGATION_INTERVAL = 60;
    private static final long TARGET_TIMESTAMP = 1800000000L;
    private static final String VALID_CELL_KEY = "cell#55.5#37.5";
    public static final double MIN_LAT = 55.0;
    public static final double MAX_LAT = 56.0;
    public static final double MIN_LON = 37.0;
    public static final double MAX_LON = 38.0;

    @Mock
    private TelemetryQueryRepository telemetryQueryRepositoryAdapter;

    @InjectMocks
    private TelemetryStateQueryResolver resolver;

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(resolver, "aggregationSecondsPerInterval", AGGREGATION_INTERVAL);
    }

    @Test
    void shouldReturnJsonWeatherMap_whenCoordinatesAreValidAndDataExists() {
        Map<String, byte[]> rawData = new HashMap<>();
        GridCellLayers layers = GridCellLayers.newBuilder().setAvgTemperature(25.5).build();
        rawData.put(VALID_CELL_KEY, layers.toByteArray());
        when(telemetryQueryRepositoryAdapter.findFilteredGridDataBySpatialBox(anyLong(), anyDouble(), anyDouble(), anyDouble(), anyDouble())).thenReturn(rawData);

        WeatherMap mapByCoordinates = resolver.getLatestTimeIntervalWeatherMapByCoordinates(TARGET_TIMESTAMP, MIN_LAT, MAX_LAT, MIN_LON, MAX_LON);

        assertTrue(mapByCoordinates.containsGridCells(VALID_CELL_KEY));
    }

    @Test
    void shouldFilterOutCellsOutsideCoordinatesSquare_whenDataIsProcessed() {
        Map<String, byte[]> rawData = new HashMap<>();
        GridCellLayers validLayers = GridCellLayers.newBuilder().setAvgTemperature(25.5).build();
        rawData.put(VALID_CELL_KEY, validLayers.toByteArray());
        when(telemetryQueryRepositoryAdapter.findFilteredGridDataBySpatialBox(anyLong(), anyDouble(), anyDouble(), anyDouble(), anyDouble())).thenReturn(rawData);

        WeatherMap mapByCoordinates = resolver.getLatestTimeIntervalWeatherMapByCoordinates(TARGET_TIMESTAMP, MIN_LAT, MAX_LAT, MIN_LON, MAX_LON);

        assertTrue(mapByCoordinates.containsGridCells(VALID_CELL_KEY));
        assertFalse(mapByCoordinates.containsGridCells("cell#60.0#40.0"));
    }

    @Test
    void shouldThrowWeatherMapDataNotFoundException_whenMinLatExceedsMaxLat() {
        when(telemetryQueryRepositoryAdapter.findFilteredGridDataBySpatialBox(anyLong(), anyDouble(), anyDouble(), anyDouble(), anyDouble())).thenReturn(Collections.emptyMap());

        assertThatThrownBy(() -> resolver.getLatestTimeIntervalWeatherMapByCoordinates(TARGET_TIMESTAMP, MAX_LAT, MIN_LAT, MIN_LON, MAX_LON))
                .isInstanceOf(WeatherMapDataNotFoundException.class);
    }

    @Test
    void shouldThrowWeatherMapDataNotFoundException_whenMinLonExceedsMaxLon() {
        when(telemetryQueryRepositoryAdapter.findFilteredGridDataBySpatialBox(anyLong(), anyDouble(), anyDouble(), anyDouble(), anyDouble())).thenReturn(Collections.emptyMap());

        assertThatThrownBy(() -> resolver.getLatestTimeIntervalWeatherMapByCoordinates(TARGET_TIMESTAMP, MIN_LAT, MAX_LAT, MAX_LON, MIN_LON))
                .isInstanceOf(WeatherMapDataNotFoundException.class);
    }

    @Test
    void shouldThrowProtocolBufferTranslationException_whenGridValueBytesAreCorrupted() {
        Map<String, byte[]> rawData = new HashMap<>();
        rawData.put(VALID_CELL_KEY, new byte[]{ 0, 1, 2 });
        when(telemetryQueryRepositoryAdapter.findFilteredGridDataBySpatialBox(anyLong(), anyDouble(), anyDouble(), anyDouble(), anyDouble())).thenReturn(rawData);

        assertThatThrownBy(() -> resolver.getLatestTimeIntervalWeatherMapByCoordinates(TARGET_TIMESTAMP, MIN_LAT, MAX_LAT, MIN_LON, MAX_LON))
                .isInstanceOf(ProtocolBufferTranslationException.class)
                .hasCauseInstanceOf(InvalidProtocolBufferException.class);
    }

    @Test
    void shouldThrowWeatherMapDataNotFoundException_whenRepositoryReturnsEmptyMatrixMap() {
        when(telemetryQueryRepositoryAdapter.findFilteredGridDataBySpatialBox(anyLong(), anyDouble(), anyDouble(), anyDouble(), anyDouble())).thenReturn(Collections.emptyMap());

        assertThatThrownBy(() -> resolver.getLatestTimeIntervalWeatherMapByCoordinates(TARGET_TIMESTAMP, MIN_LAT, MAX_LAT, MIN_LON, MAX_LON))
                .isInstanceOf(WeatherMapDataNotFoundException.class);
    }
}
