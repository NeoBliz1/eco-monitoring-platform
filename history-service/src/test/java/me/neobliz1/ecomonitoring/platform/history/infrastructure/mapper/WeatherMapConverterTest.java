package me.neobliz1.ecomonitoring.platform.history.infrastructure.mapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import me.neobliz1.ecomonitoring.platform.history.domain.model.entity.WeatherGridCellLayer;
import me.neobliz1.ecomonitoring.platform.history.domain.model.entity.WeatherMapBucket;
import me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter.outbound.persistence.postgres.jpa.HistoricalWeatherGridCellJpaRepository;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.GridCellLayers;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.WeatherMap;
import me.neobliz1.ecomonitoring.platform.test.common.util.WeatherTestUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.UUID;

@ExtendWith(MockitoExtension.class)
class WeatherMapConverterTest {

    @Mock
    private HistoricalWeatherGridCellJpaRepository gridCellJpaRepository;

    @Test
    void shouldSaveOneNewCell_whenWeatherMapContainsGridCells() {
        WeatherMapConverter converter = new WeatherMapConverter(gridCellJpaRepository);
        stubNoExistingCells();
        ArgumentCaptor<List<WeatherGridCellLayer>> captor = cellsCaptor();

        converter.mergeTelemetryInBatch(WeatherTestUtils.getWeatherMap(), bucketWithId());

        verify(gridCellJpaRepository).saveAllAndFlush(captor.capture());
        assertThat(captor.getValue()).hasSize(1);
    }

    @Test
    void shouldCopyEveryMetricOntoNewCell_whenNoExistingCellMatches() {
        WeatherMapConverter converter = new WeatherMapConverter(gridCellJpaRepository);
        stubNoExistingCells();
        ArgumentCaptor<List<WeatherGridCellLayer>> captor = cellsCaptor();

        converter.mergeTelemetryInBatch(WeatherTestUtils.getWeatherMap(), bucketWithId());

        verify(gridCellJpaRepository).saveAllAndFlush(captor.capture());
        assertCellMatchesExpectedMetrics(captor.getValue().getFirst());
    }

    @Test
    void shouldMergeIntoExistingCell_whenExistingCellMatchesGeohash() {
        WeatherMapConverter converter = new WeatherMapConverter(gridCellJpaRepository);
        WeatherGridCellLayer existing = fullyPopulatedCell(bucketWithId());
        stubExistingCell(existing);
        ArgumentCaptor<List<WeatherGridCellLayer>> captor = cellsCaptor();

        converter.mergeTelemetryInBatch(WeatherTestUtils.getWeatherMap(), bucketWithId());

        verify(gridCellJpaRepository).saveAllAndFlush(captor.capture());
        assertThat(captor.getValue().getFirst()).isSameAs(existing);
        assertThat(existing.getReadingCount()).isEqualTo(WeatherTestUtils.VAL_COUNT*2);
    }

    @Test
    void shouldNotQueryRepository_whenWeatherMapHasNoGridCells() {
        WeatherMapConverter converter = new WeatherMapConverter(gridCellJpaRepository);
        WeatherMap emptyMap = WeatherMap.newBuilder().build();

        converter.mergeTelemetryInBatch(emptyMap, bucketWithId());

        verify(gridCellJpaRepository, never()).findSpecificGridCellLayersForMerge(any(), anySet());
        verify(gridCellJpaRepository, never()).saveAllAndFlush(any());
    }

    @Test
    void shouldConvertEntityToProto_whenAllMetricsArePresent() {
        WeatherMapConverter converter = new WeatherMapConverter(gridCellJpaRepository);
        WeatherGridCellLayer cell = fullyPopulatedCell(bucketWithId());

        GridCellLayers result = converter.convertWeatherGridCellsToWeatherMap(cell);

        assertProtoMatchesExpectedMetrics(result);
    }

    private void assertCellMatchesExpectedMetrics(WeatherGridCellLayer cell) {
        assertThat(cell.getBucketId()).isNotNull();
        assertThat(cell.getGeohash()).isEqualTo(WeatherTestUtils.GEOHASH_ALPHA);
        assertThat(cell.getReadingCount()).isEqualTo(WeatherTestUtils.VAL_COUNT);
        assertThat(cell.getAvgTemperature()).isEqualTo(WeatherTestUtils.VAL_TEMP);
        assertThat(cell.getAvgHumidity()).isEqualTo(WeatherTestUtils.VAL_HUMIDITY);
        assertThat(cell.getAvgPressure()).isEqualTo(WeatherTestUtils.VAL_PRESSURE);
        assertThat(cell.getAvgLeaf_wetnessPct()).isEqualTo(WeatherTestUtils.VAL_LEAF);
        assertThat(cell.getAvgWindSpeed()).isEqualTo(WeatherTestUtils.VAL_WIND_SPEED);
        assertThat(cell.getAvgWindDirection()).isEqualTo(WeatherTestUtils.VAL_WIND_DIR);
        assertThat(cell.getAvgPm25()).isEqualTo(WeatherTestUtils.VAL_PM25);
        assertThat(cell.getAvgPm10()).isEqualTo(WeatherTestUtils.VAL_PM10);
        assertThat(cell.getAvgPm100()).isEqualTo(WeatherTestUtils.VAL_PM100);
        assertThat(cell.getAvgVoc()).isEqualTo(WeatherTestUtils.VAL_VOC);
        assertThat(cell.getAvgNoiseDb()).isEqualTo(WeatherTestUtils.VAL_NOISE);
        assertThat(cell.getAvgRainMm()).isEqualTo(WeatherTestUtils.VAL_RAIN);
        assertThat(cell.getAvgSnowCm()).isEqualTo(WeatherTestUtils.VAL_SNOW);
        assertThat(cell.getAvgEvapRate()).isEqualTo(WeatherTestUtils.VAL_EVAP);
        assertThat(cell.getAvgUvIndex()).isEqualTo(WeatherTestUtils.VAL_UV);
        assertThat(cell.getAvgSolarRadiationWm2()).isEqualTo(WeatherTestUtils.VAL_SOLAR);
        assertThat(cell.getAvgLux()).isEqualTo(WeatherTestUtils.VAL_LUX);
        assertThat(cell.getAvgVisibilityM()).isEqualTo(WeatherTestUtils.VAL_VIS);
    }

    private void assertProtoMatchesExpectedMetrics(GridCellLayers result) {
        assertThat(result.getGeohash()).isEqualTo(WeatherTestUtils.GEOHASH_ALPHA);
        assertThat(result.getReadingCount()).isEqualTo(WeatherTestUtils.VAL_COUNT);
        assertThat(result.getAvgTemperature()).isEqualTo(WeatherTestUtils.VAL_TEMP);
        assertThat(result.getAvgHumidity()).isEqualTo(WeatherTestUtils.VAL_HUMIDITY);
        assertThat(result.getAvgPressure()).isEqualTo(WeatherTestUtils.VAL_PRESSURE);
        assertThat(result.getAvgLeafWetnessPct()).isEqualTo(WeatherTestUtils.VAL_LEAF);
        assertThat(result.getAvgWindSpeed()).isEqualTo(WeatherTestUtils.VAL_WIND_SPEED);
        assertThat(result.getAvgWindDirection()).isEqualTo(WeatherTestUtils.VAL_WIND_DIR);
        assertThat(result.getAvgPm25()).isEqualTo(WeatherTestUtils.VAL_PM25);
        assertThat(result.getAvgPm10()).isEqualTo(WeatherTestUtils.VAL_PM10);
        assertThat(result.getAvgPm100()).isEqualTo(WeatherTestUtils.VAL_PM100);
        assertThat(result.getAvgVoc()).isEqualTo(WeatherTestUtils.VAL_VOC);
        assertThat(result.getAvgNoiseDb()).isEqualTo(WeatherTestUtils.VAL_NOISE);
        assertThat(result.getAvgRainMm()).isEqualTo(WeatherTestUtils.VAL_RAIN);
        assertThat(result.getAvgSnowCm()).isEqualTo(WeatherTestUtils.VAL_SNOW);
        assertThat(result.getAvgEvapRate()).isEqualTo(WeatherTestUtils.VAL_EVAP);
        assertThat(result.getAvgUvIndex()).isEqualTo(WeatherTestUtils.VAL_UV);
        assertThat(result.getAvgSolarRadiationWm2()).isEqualTo(WeatherTestUtils.VAL_SOLAR);
        assertThat(result.getAvgLux()).isEqualTo(WeatherTestUtils.VAL_LUX);
        assertThat(result.getAvgVisibilityM()).isEqualTo(WeatherTestUtils.VAL_VIS);
    }

    private void stubNoExistingCells() {
        when(gridCellJpaRepository.findSpecificGridCellLayersForMerge(any(), anySet()))
                .thenReturn(List.of());
    }

    private void stubExistingCell(WeatherGridCellLayer cell) {
        when(gridCellJpaRepository.findSpecificGridCellLayersForMerge(any(), anySet()))
                .thenReturn(List.of(cell));
    }

    @SuppressWarnings("unchecked")
    private ArgumentCaptor<List<WeatherGridCellLayer>> cellsCaptor() {
        return ArgumentCaptor.forClass(List.class);
    }

    private WeatherMapBucket bucketWithId() {
        WeatherMapBucket bucket = new WeatherMapBucket();
        bucket.setId(UUID.randomUUID());
        return bucket;
    }

    private WeatherGridCellLayer fullyPopulatedCell(WeatherMapBucket bucket) {
        WeatherGridCellLayer cell = new WeatherGridCellLayer(bucket, WeatherTestUtils.GEOHASH_ALPHA);
        cell.setReadingCount(WeatherTestUtils.VAL_COUNT);
        cell.setAvgTemperature(WeatherTestUtils.VAL_TEMP);
        cell.setAvgHumidity(WeatherTestUtils.VAL_HUMIDITY);
        cell.setAvgPressure(WeatherTestUtils.VAL_PRESSURE);
        cell.setAvgLeaf_wetnessPct(WeatherTestUtils.VAL_LEAF);
        cell.setAvgWindSpeed(WeatherTestUtils.VAL_WIND_SPEED);
        cell.setAvgWindDirection(WeatherTestUtils.VAL_WIND_DIR);
        cell.setAvgPm25(WeatherTestUtils.VAL_PM25);
        cell.setAvgPm10(WeatherTestUtils.VAL_PM10);
        cell.setAvgPm100(WeatherTestUtils.VAL_PM100);
        cell.setAvgVoc(WeatherTestUtils.VAL_VOC);
        cell.setAvgNoiseDb(WeatherTestUtils.VAL_NOISE);
        cell.setAvgRainMm(WeatherTestUtils.VAL_RAIN);
        cell.setAvgSnowCm(WeatherTestUtils.VAL_SNOW);
        cell.setAvgEvapRate(WeatherTestUtils.VAL_EVAP);
        cell.setAvgUvIndex(WeatherTestUtils.VAL_UV);
        cell.setAvgSolarRadiationWm2(WeatherTestUtils.VAL_SOLAR);
        cell.setAvgLux(WeatherTestUtils.VAL_LUX);
        cell.setAvgVisibilityM(WeatherTestUtils.VAL_VIS);
        return cell;
    }
}