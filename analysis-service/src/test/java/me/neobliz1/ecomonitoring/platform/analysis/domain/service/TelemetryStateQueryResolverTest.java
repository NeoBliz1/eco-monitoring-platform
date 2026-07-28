package me.neobliz1.ecomonitoring.platform.analysis.domain.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import com.google.protobuf.InvalidProtocolBufferException;
import me.neobliz1.ecomonitoring.platform.analysis.domain.port.outbound.TelemetryQueryRepository;
import me.neobliz1.ecomonitoring.platform.model.exception.InvalidCoordinatesSquareException;
import me.neobliz1.ecomonitoring.platform.model.exception.ProtocolBufferTranslationException;
import me.neobliz1.ecomonitoring.platform.model.exception.WeatherMapDataNotFoundException;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.GridCellLayers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@ExtendWith(MockitoExtension.class)
class TelemetryStateQueryResolverTest {

    private static final int AGGREGATION_INTERVAL = 60;
    private static final long TARGET_TIMESTAMP = 1800000000L;
    private static final String VALID_CELL_KEY = "cell#55.5#37.5";

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
        List<Double> coordinates = List.of(55.0, 56.0, 37.0, 38.0);
        Map<Object, Object> rawData = new HashMap<>();
        GridCellLayers layers = GridCellLayers.newBuilder().setAvgTemperature(25.5).build();
        rawData.put(VALID_CELL_KEY, layers.toByteArray());
        when(telemetryQueryRepositoryAdapter.findRawGridDataByBucketFloor(anyString())).thenReturn(rawData);

        String result = resolver.getLatestTimeIntervalWeatherMapByCoordinates(TARGET_TIMESTAMP, coordinates);

        assertThat(result).contains("timestamp_bucket");
        assertThat(result).contains("grid_cells");
    }

    @Test
    void shouldFilterOutCellsOutsideCoordinatesSquare_whenDataIsProcessed() {
        List<Double> coordinates = List.of(55.0, 56.0, 37.0, 38.0);
        Map<Object, Object> rawData = new HashMap<>();
        GridCellLayers validLayers = GridCellLayers.newBuilder().setAvgTemperature(25.5).build();
        GridCellLayers invalidLayers = GridCellLayers.newBuilder().setAvgTemperature(10.0).build();
        rawData.put(VALID_CELL_KEY, validLayers.toByteArray());
        rawData.put("cell#60.0#40.0", invalidLayers.toByteArray());
        when(telemetryQueryRepositoryAdapter.findRawGridDataByBucketFloor(anyString())).thenReturn(rawData);

        String result = resolver.getLatestTimeIntervalWeatherMapByCoordinates(TARGET_TIMESTAMP, coordinates);

        assertThat(result).contains("55.5");
        assertThat(result).doesNotContain("60.0");
    }

    @Test
    void shouldThrowInvalidCoordinatesSquareException_whenCoordinatesListIsNull() {
        List<Double> coordinates = null;

        assertThatThrownBy(() -> resolver.getLatestTimeIntervalWeatherMapByCoordinates(TARGET_TIMESTAMP, coordinates))
                .isInstanceOf(InvalidCoordinatesSquareException.class);
    }

    @Test
    void shouldThrowInvalidCoordinatesSquareException_whenCoordinatesListHasInsufficientElements() {
        List<Double> coordinates = List.of(55.0, 56.0, 37.0);

        assertThatThrownBy(() -> resolver.getLatestTimeIntervalWeatherMapByCoordinates(TARGET_TIMESTAMP, coordinates))
                .isInstanceOf(InvalidCoordinatesSquareException.class);
    }

    @Test
    void shouldThrowInvalidCoordinatesSquareException_whenCoordinatesDoNotFormValidSquareRange() {
        List<Double> coordinates = List.of(56.0, 55.0, 37.0, 38.0);

        assertThatThrownBy(() -> resolver.getLatestTimeIntervalWeatherMapByCoordinates(TARGET_TIMESTAMP, coordinates))
                .isInstanceOf(InvalidCoordinatesSquareException.class);
    }

    @Test
    void shouldThrowProtocolBufferTranslationException_whenGridKeyCoordinatesAreMalformed() {
        List<Double> coordinates = List.of(55.0, 56.0, 37.0, 38.0);
        Map<Object, Object> rawData = new HashMap<>();
        rawData.put("cell#not_a_double#37.5", new byte[]{ 1, 2, 3 });
        when(telemetryQueryRepositoryAdapter.findRawGridDataByBucketFloor(anyString())).thenReturn(rawData);

        assertThatThrownBy(() -> resolver.getLatestTimeIntervalWeatherMapByCoordinates(TARGET_TIMESTAMP, coordinates))
                .isInstanceOf(ProtocolBufferTranslationException.class)
                .hasMessageContaining("Corrupted or malformed grid matrix coordinates key");
    }

    @Test
    void shouldThrowProtocolBufferTranslationException_whenGridValueBytesAreCorrupted() {
        List<Double> coordinates = List.of(55.0, 56.0, 37.0, 38.0);
        Map<Object, Object> rawData = new HashMap<>();
        rawData.put(VALID_CELL_KEY, new byte[]{ 0, 1, 2 });
        when(telemetryQueryRepositoryAdapter.findRawGridDataByBucketFloor(anyString())).thenReturn(rawData);

        assertThatThrownBy(() -> resolver.getLatestTimeIntervalWeatherMapByCoordinates(TARGET_TIMESTAMP, coordinates))
                .isInstanceOf(ProtocolBufferTranslationException.class)
                .hasCauseInstanceOf(InvalidProtocolBufferException.class);
    }

    @Test
    void shouldThrowWeatherMapDataNotFoundException_whenNoGridCellsMatchCoordinatesSquare() {
        List<Double> coordinates = List.of(55.0, 56.0, 37.0, 38.0);
        Map<Object, Object> rawData = new HashMap<>();
        GridCellLayers layers = GridCellLayers.newBuilder().setAvgTemperature(20.0).build();
        rawData.put("cell#10.0#10.0", layers.toByteArray());
        when(telemetryQueryRepositoryAdapter.findRawGridDataByBucketFloor(anyString())).thenReturn(rawData);

        assertThatThrownBy(() -> resolver.getLatestTimeIntervalWeatherMapByCoordinates(TARGET_TIMESTAMP, coordinates))
                .isInstanceOf(WeatherMapDataNotFoundException.class);
    }

    @Test
    void shouldThrowWeatherMapDataNotFoundException_whenRepositoryReturnsEmptyMatrixMap() {
        List<Double> coordinates = List.of(55.0, 56.0, 37.0, 38.0);
        when(telemetryQueryRepositoryAdapter.findRawGridDataByBucketFloor(anyString())).thenReturn(Collections.emptyMap());

        assertThatThrownBy(() -> resolver.getLatestTimeIntervalWeatherMapByCoordinates(TARGET_TIMESTAMP, coordinates))
                .isInstanceOf(WeatherMapDataNotFoundException.class);
    }
}
