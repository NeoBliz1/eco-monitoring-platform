package me.neobliz1.ecomonitoring.platform.history.infrastructure.mapper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verify;

import me.neobliz1.ecomonitoring.platform.history.domain.model.entity.WeatherGridCellMetric;
import me.neobliz1.ecomonitoring.platform.history.domain.model.entity.WeatherMapBucket;
import me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter.outbound.persistence.postgres.jpa.HistoricalWeatherGridCellJpaRepository;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.WeatherMap;
import me.neobliz1.ecomonitoring.platform.test.common.util.WeatherTestUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

@ExtendWith(MockitoExtension.class)
class WeatherMapConverterTest {

    @Mock
    private HistoricalWeatherGridCellJpaRepository gridCellJpaRepository;

    @Test
    void shouldExtractAllTelemetryMetricsAndSaveThemViaRepository_whenWeatherMapContainsGridCells() {
        WeatherMapConverter converter = new WeatherMapConverter(gridCellJpaRepository);
        WeatherMapBucket bucket = new WeatherMapBucket();
        bucket.setId(UUID.randomUUID());
        WeatherMap weatherMap = WeatherTestUtils.getWeatherMap();
        //noinspection unchecked
        ArgumentCaptor<List<WeatherGridCellMetric>> cellsCaptor = ArgumentCaptor.forClass(List.class);

        converter.mergeTelemetryInBatch(weatherMap, bucket, new ArrayList<>());

        verify(gridCellJpaRepository).saveAll(cellsCaptor.capture());
        List<WeatherGridCellMetric> savedCells = cellsCaptor.getValue();
        assertEquals(1, savedCells.size());
        WeatherGridCellMetric metric = savedCells.getFirst();
        assertNotNull(metric.getBucketId());
        assertEquals(WeatherTestUtils.GEOHASH_ALPHA, metric.getGeohash());
        assertEquals(WeatherTestUtils.VAL_COUNT, metric.getReadingCount());
        assertEquals(WeatherTestUtils.VAL_TEMP, metric.getAvgTemperature());
        assertEquals(WeatherTestUtils.VAL_HUMIDITY, metric.getAvgHumidity());
        assertEquals(WeatherTestUtils.VAL_PRESSURE, metric.getAvgPressure());
        assertEquals(WeatherTestUtils.VAL_LEAF, metric.getAvgLeaf_wetnessPct());
        assertEquals(WeatherTestUtils.VAL_WIND_SPEED, metric.getAvgWindSpeed());
        assertEquals(WeatherTestUtils.VAL_WIND_DIR, metric.getAvgWindDirection());
        assertEquals(WeatherTestUtils.VAL_PM25, metric.getAvgPm25());
        assertEquals(WeatherTestUtils.VAL_PM10, metric.getAvgPm10());
        assertEquals(WeatherTestUtils.VAL_PM100, metric.getAvgPm100());
        assertEquals(WeatherTestUtils.VAL_VOC, metric.getAvgVoc());
        assertEquals(WeatherTestUtils.VAL_NOISE, metric.getAvgNoiseDb());
        assertEquals(WeatherTestUtils.VAL_RAIN, metric.getAvgRainMm());
        assertEquals(WeatherTestUtils.VAL_SNOW, metric.getAvgSnowCm());
        assertEquals(WeatherTestUtils.VAL_EVAP, metric.getAvgEvapRate());
        assertEquals(WeatherTestUtils.VAL_UV, metric.getAvgUvIndex());
        assertEquals(WeatherTestUtils.VAL_SOLAR, metric.getAvgSolarRadiationWm2());
        assertEquals(WeatherTestUtils.VAL_LUX, metric.getAvgLux());
        assertEquals(WeatherTestUtils.VAL_VIS, metric.getAvgVisibilityM());
    }

    @Test
    void shouldLeaveBucketEmpty_whenWeatherMapContainsNoGridCells() {
        WeatherMapConverter converter = new WeatherMapConverter(gridCellJpaRepository);
        WeatherMapBucket bucket = new WeatherMapBucket();
        WeatherMap weatherMap = WeatherMap.newBuilder().build();

        converter.mergeTelemetryInBatch(weatherMap, bucket, Collections.emptyList());

        assertTrue(bucket.getGridCells().isEmpty());
    }
}
