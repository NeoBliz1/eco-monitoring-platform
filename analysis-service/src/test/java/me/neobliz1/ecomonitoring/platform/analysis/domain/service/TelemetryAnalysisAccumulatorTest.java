package me.neobliz1.ecomonitoring.platform.analysis.domain.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.AirQualityReading;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.AmbientReading;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.OpticalReading;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.PrecipitationReading;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.SensorReading;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.WindReading;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.GridCellLayers;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class TelemetryAnalysisAccumulatorTest {

    private static final float TEST_TEMP = 22.5f;
    private static final float TEST_HUMIDITY = 65.0f;
    private static final float TEST_PRESSURE = 1013.25f;
    private static final float TEST_LEAF_WETNESS = 12.0f;
    private static final float TEST_WIND_SPEED = 5.5f;
    private static final int TEST_WIND_DIRECTION = 90;
    private static final float TEST_PM100 = 15.0f;
    private static final float TEST_PM25 = 8.0f;
    private static final float TEST_PM10 = 4.0f;
    private static final float TEST_VOC = 100.0f;
    private static final float TEST_NOISE = 45.0f;
    private static final float TEST_RAIN = 2.5f;
    private static final float TEST_SNOW = 0.0f;
    private static final float TEST_EVAPORATE = 0.1f;
    private static final float TEST_UV = 3.0f;
    private static final float TEST_SOLAR = 450.0f;
    private static final float TEST_LUX = 12000.0f;
    private static final float TEST_VISIBILITY = 10000.0f;

    @Test
    void shouldAccumulateAmbientData_whenSensorReadingIsAmbient() {
        TelemetryAnalysisAccumulator accumulator = new TelemetryAnalysisAccumulator();
        AmbientReading ambientReading = Mockito.mock(AmbientReading.class);
        SensorReading reading = Mockito.mock(SensorReading.class);
        when(reading.getSensorDataCase()).thenReturn(SensorReading.SensorDataCase.AMBIENT);
        when(reading.getAmbient()).thenReturn(ambientReading);
        when(ambientReading.getTemperatureC()).thenReturn(TEST_TEMP);
        when(ambientReading.getHumidityPct()).thenReturn(TEST_HUMIDITY);
        when(ambientReading.getPressureHpa()).thenReturn(TEST_PRESSURE);
        when(ambientReading.getLeafWetnessPct()).thenReturn(TEST_LEAF_WETNESS);

        accumulator.accumulate(reading);

        assertThat(accumulator.getTemp().getAverage()).isEqualTo(TEST_TEMP);
        assertThat(accumulator.getHumidity().getAverage()).isEqualTo(TEST_HUMIDITY);
        assertThat(accumulator.getPressure().getAverage()).isEqualTo(TEST_PRESSURE);
        assertThat(accumulator.getLeafWetness().getAverage()).isEqualTo(TEST_LEAF_WETNESS);
    }

    @Test
    void shouldAccumulateWindAndCalculateTrigonometricVectors_whenSensorReadingIsWind() {
        TelemetryAnalysisAccumulator accumulator = new TelemetryAnalysisAccumulator();
        WindReading windReading = Mockito.mock(WindReading.class);
        SensorReading reading = Mockito.mock(SensorReading.class);
        when(reading.getSensorDataCase()).thenReturn(SensorReading.SensorDataCase.WIND);
        when(reading.getWind()).thenReturn(windReading);
        when(windReading.getSpeedMps()).thenReturn(TEST_WIND_SPEED);
        when(windReading.getDirectionDeg()).thenReturn(TEST_WIND_DIRECTION);
        double rad = Math.toRadians(TEST_WIND_DIRECTION);
        double expectedSin = Math.sin(rad);
        double expectedCos = Math.cos(rad);

        accumulator.accumulate(reading);

        assertThat(accumulator.getWindSpeed().getAverage()).isEqualTo(TEST_WIND_SPEED);
        assertThat(accumulator.getWindSin().getAverage()).isEqualTo(expectedSin);
        assertThat(accumulator.getWindCos().getAverage()).isEqualTo(expectedCos);
    }

    @Test
    void shouldAccumulateAirQualityMetrics_whenSensorReadingIsAirQuality() {
        TelemetryAnalysisAccumulator accumulator = new TelemetryAnalysisAccumulator();
        AirQualityReading airQualityReading = Mockito.mock(AirQualityReading.class);
        SensorReading reading = Mockito.mock(SensorReading.class);
        when(reading.getSensorDataCase()).thenReturn(SensorReading.SensorDataCase.AIR_QUALITY);
        when(reading.getAirQuality()).thenReturn(airQualityReading);
        when(airQualityReading.getPm100()).thenReturn(TEST_PM100);
        when(airQualityReading.getPm25()).thenReturn(TEST_PM25);
        when(airQualityReading.getPm10()).thenReturn(TEST_PM10);
        when(airQualityReading.getVocIndex()).thenReturn(TEST_VOC);
        when(airQualityReading.getNoiseDb()).thenReturn(TEST_NOISE);

        accumulator.accumulate(reading);

        assertThat(accumulator.getPm100().getAverage()).isEqualTo(TEST_PM100);
        assertThat(accumulator.getPm25().getAverage()).isEqualTo(TEST_PM25);
        assertThat(accumulator.getPm10().getAverage()).isEqualTo(TEST_PM10);
        assertThat(accumulator.getVoc().getAverage()).isEqualTo(TEST_VOC);
        assertThat(accumulator.getNoise().getAverage()).isEqualTo(TEST_NOISE);
    }

    @Test
    void shouldAccumulatePrecipitationMetrics_whenSensorReadingIsPrecipitation() {
        TelemetryAnalysisAccumulator accumulator = new TelemetryAnalysisAccumulator();
        PrecipitationReading precipitationReading = Mockito.mock(PrecipitationReading.class);
        SensorReading reading = Mockito.mock(SensorReading.class);
        when(reading.getSensorDataCase()).thenReturn(SensorReading.SensorDataCase.PRECIPITATION);
        when(reading.getPrecipitation()).thenReturn(precipitationReading);
        when(precipitationReading.getRainRateMmH()).thenReturn(TEST_RAIN);
        when(precipitationReading.getSnowDepthCm()).thenReturn(TEST_SNOW);
        when(precipitationReading.getEvaporationRate()).thenReturn(TEST_EVAPORATE);

        accumulator.accumulate(reading);

        assertThat(accumulator.getRain().getAverage()).isEqualTo(TEST_RAIN);
        assertThat(accumulator.getSnow().getAverage()).isEqualTo(TEST_SNOW);
        assertThat(accumulator.getEvaporate().getAverage()).isEqualTo(TEST_EVAPORATE);
    }

    @Test
    void shouldAccumulateOpticalMetrics_whenSensorReadingIsOptical() {
        TelemetryAnalysisAccumulator accumulator = new TelemetryAnalysisAccumulator();
        OpticalReading opticalReading = Mockito.mock(OpticalReading.class);
        SensorReading reading = Mockito.mock(SensorReading.class);
        when(reading.getSensorDataCase()).thenReturn(SensorReading.SensorDataCase.OPTICAL);
        when(reading.getOptical()).thenReturn(opticalReading);
        when(opticalReading.getUvIndex()).thenReturn(TEST_UV);
        when(opticalReading.getSolarRadiationWm2()).thenReturn(TEST_SOLAR);
        when(opticalReading.getLux()).thenReturn(TEST_LUX);
        when(opticalReading.getVisibilityM()).thenReturn(TEST_VISIBILITY);

        accumulator.accumulate(reading);

        assertThat(accumulator.getUv().getAverage()).isEqualTo(TEST_UV);
        assertThat(accumulator.getSolar().getAverage()).isEqualTo(TEST_SOLAR);
        assertThat(accumulator.getLux().getAverage()).isEqualTo(TEST_LUX);
        assertThat(accumulator.getVis().getAverage()).isEqualTo(TEST_VISIBILITY);
    }

    @Test
    void shouldMergeAllInternalStatistics_whenCombiningWithAnotherAccumulator() {
        TelemetryAnalysisAccumulator baseAccumulator = new TelemetryAnalysisAccumulator();
        TelemetryAnalysisAccumulator secondaryAccumulator = new TelemetryAnalysisAccumulator();
        baseAccumulator.getTemp().accept(10.0);
        secondaryAccumulator.getTemp().accept(20.0);

        baseAccumulator.merge(secondaryAccumulator);

        assertThat(baseAccumulator.getTemp().getCount()).isEqualTo(2);
        assertThat(baseAccumulator.getTemp().getAverage()).isEqualTo(15.0);
    }

    @Test
    void shouldMapAggregatedAveragesToBuilder_whenLayerCountsAreGreaterThanZero() {
        TelemetryAnalysisAccumulator accumulator = new TelemetryAnalysisAccumulator();
        GridCellLayers.Builder builder = Mockito.mock(GridCellLayers.Builder.class);
        accumulator.getTemp().accept(TEST_TEMP);
        accumulator.getHumidity().accept(TEST_HUMIDITY);
        accumulator.getPressure().accept(TEST_PRESSURE);
        accumulator.getLeafWetness().accept(TEST_LEAF_WETNESS);

        accumulator.applyTo(builder);

        verify(builder).setAvgTemperature(TEST_TEMP);
        verify(builder).setAvgHumidity(TEST_HUMIDITY);
        verify(builder).setAvgPressure(TEST_PRESSURE);
        verify(builder).setAvgLeafWetnessPct(TEST_LEAF_WETNESS);
    }

    @Test
    void shouldCalculateTrueDirectionDegAndMapToBuilder_whenWindVectorIsProcessed() {
        TelemetryAnalysisAccumulator accumulator = new TelemetryAnalysisAccumulator();
        GridCellLayers.Builder builder = Mockito.mock(GridCellLayers.Builder.class);
        accumulator.getWindSpeed().accept(10.0);
        accumulator.getWindSin().accept(0.0);
        accumulator.getWindCos().accept(-1.0);

        accumulator.applyTo(builder);

        verify(builder).setAvgWindSpeed(10.0);
        verify(builder).setAvgWindDirection(180);
    }

    @Test
    void shouldAdd360ToDegrees_whenAtan2ReturnsNegativeAngle() {
        TelemetryAnalysisAccumulator accumulator = new TelemetryAnalysisAccumulator();
        GridCellLayers.Builder builder = Mockito.mock(GridCellLayers.Builder.class);
        accumulator.getWindSpeed().accept(10.0);
        accumulator.getWindSin().accept(-0.5);
        accumulator.getWindCos().accept(0.866);

        accumulator.applyTo(builder);

        verify(builder).setAvgWindDirection(330);
    }

    @Test
    void shouldDoNothingToStatistics_whenSensorReadingIsDataNotSet() {
        TelemetryAnalysisAccumulator accumulator = new TelemetryAnalysisAccumulator();
        SensorReading reading = Mockito.mock(SensorReading.class);
        when(reading.getSensorDataCase()).thenReturn(SensorReading.SensorDataCase.SENSORDATA_NOT_SET);

        accumulator.accumulate(reading);

        assertThat(accumulator.getTemp().getCount()).isZero();
        assertThat(accumulator.getWindSpeed().getCount()).isZero();
    }

    @Test
    void shouldSkipBuilderProperties_whenLayerCountsAreZero() {
        TelemetryAnalysisAccumulator accumulator = new TelemetryAnalysisAccumulator();
        GridCellLayers.Builder builder = Mockito.mock(GridCellLayers.Builder.class);
        accumulator.applyTo(builder);
        verifyNoInteractions(builder);
    }
}