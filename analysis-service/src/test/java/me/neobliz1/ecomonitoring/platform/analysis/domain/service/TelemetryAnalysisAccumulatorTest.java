package me.neobliz1.ecomonitoring.platform.analysis.domain.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import me.neobliz1.ecomonitoring.platform.analysis.domain.model.accumulator.AirQualityAccumulator;
import me.neobliz1.ecomonitoring.platform.analysis.domain.model.accumulator.AmbientAccumulator;
import me.neobliz1.ecomonitoring.platform.analysis.domain.model.accumulator.OpticalAccumulator;
import me.neobliz1.ecomonitoring.platform.analysis.domain.model.accumulator.PortSensorDataCase;
import me.neobliz1.ecomonitoring.platform.analysis.domain.model.accumulator.PrecipitationAccumulator;
import me.neobliz1.ecomonitoring.platform.analysis.domain.model.accumulator.WindAccumulator;
import me.neobliz1.ecomonitoring.platform.analysis.domain.port.inbound.SensorGroupAccumulator;
import me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.messaging.kafka.processor.model.PortGridCellLayersBuilder;
import me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.messaging.kafka.processor.model.PortSensorReading;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.AirQualityReading;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.AmbientReading;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.OpticalReading;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.PrecipitationReading;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.SensorReading;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.WindReading;
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

    private static PortSensorReading portReading(PortSensorDataCase dataCase, SensorReading protoReading) {
        PortSensorReading port = Mockito.mock(PortSensorReading.class);
        when(port.getSensorDataCase()).thenReturn(dataCase);
        when(port.sensorReading()).thenReturn(protoReading);
        return port;
    }

    private static <T extends SensorGroupAccumulator> T groupOf(
            TelemetryAnalysisAccumulator accumulator, Class<T> type) {
        return accumulator.getGroups().stream()
                .filter(type::isInstance)
                .map(type::cast)
                .findFirst()
                .orElseThrow(() -> new AssertionError("No group of type "+type.getSimpleName()));
    }

    @Test
    void shouldAccumulateAmbientData_whenSensorReadingIsAmbient() {
        TelemetryAnalysisAccumulator accumulator = new TelemetryAnalysisAccumulator();

        AmbientReading ambientReading = Mockito.mock(AmbientReading.class);
        when(ambientReading.getTemperatureC()).thenReturn(TEST_TEMP);
        when(ambientReading.getHumidityPct()).thenReturn(TEST_HUMIDITY);
        when(ambientReading.getPressureHpa()).thenReturn(TEST_PRESSURE);
        when(ambientReading.getLeafWetnessPct()).thenReturn(TEST_LEAF_WETNESS);

        SensorReading proto = Mockito.mock(SensorReading.class);
        when(proto.getAmbient()).thenReturn(ambientReading);

        accumulator.accumulate(portReading(PortSensorDataCase.AMBIENT, proto));

        AmbientAccumulator ambient = groupOf(accumulator, AmbientAccumulator.class);
        assertThat(ambient.getTemp().getAverage()).isEqualTo(TEST_TEMP);
        assertThat(ambient.getHumidity().getAverage()).isEqualTo(TEST_HUMIDITY);
        assertThat(ambient.getPressure().getAverage()).isEqualTo(TEST_PRESSURE);
        assertThat(ambient.getLeafWetness().getAverage()).isEqualTo(TEST_LEAF_WETNESS);
    }

    @Test
    void shouldAccumulateWindAndTrigonometricVectors_whenSensorReadingIsWind() {
        TelemetryAnalysisAccumulator accumulator = new TelemetryAnalysisAccumulator();

        WindReading windReading = Mockito.mock(WindReading.class);
        when(windReading.getSpeedMps()).thenReturn(TEST_WIND_SPEED);
        when(windReading.getDirectionDeg()).thenReturn(TEST_WIND_DIRECTION);

        SensorReading proto = Mockito.mock(SensorReading.class);
        when(proto.getWind()).thenReturn(windReading);

        accumulator.accumulate(portReading(PortSensorDataCase.WIND, proto));

        WindAccumulator wind = groupOf(accumulator, WindAccumulator.class);
        double rad = Math.toRadians(TEST_WIND_DIRECTION);
        assertThat(wind.getWindSpeed().getAverage()).isEqualTo(TEST_WIND_SPEED);
        assertThat(wind.getWindSin().getAverage()).isEqualTo(Math.sin(rad));
        assertThat(wind.getWindCos().getAverage()).isEqualTo(Math.cos(rad));
    }

    @Test
    void shouldAccumulateAirQualityMetrics_whenSensorReadingIsAirQuality() {
        TelemetryAnalysisAccumulator accumulator = new TelemetryAnalysisAccumulator();

        AirQualityReading airQualityReading = Mockito.mock(AirQualityReading.class);
        when(airQualityReading.getPm100()).thenReturn(TEST_PM100);
        when(airQualityReading.getPm25()).thenReturn(TEST_PM25);
        when(airQualityReading.getPm10()).thenReturn(TEST_PM10);
        when(airQualityReading.getVocIndex()).thenReturn(TEST_VOC);
        when(airQualityReading.getNoiseDb()).thenReturn(TEST_NOISE);

        SensorReading proto = Mockito.mock(SensorReading.class);
        when(proto.getAirQuality()).thenReturn(airQualityReading);

        accumulator.accumulate(portReading(PortSensorDataCase.AIR_QUALITY, proto));

        AirQualityAccumulator air = groupOf(accumulator, AirQualityAccumulator.class);
        assertThat(air.getPm100().getAverage()).isEqualTo(TEST_PM100);
        assertThat(air.getPm25().getAverage()).isEqualTo(TEST_PM25);
        assertThat(air.getPm10().getAverage()).isEqualTo(TEST_PM10);
        assertThat(air.getVoc().getAverage()).isEqualTo(TEST_VOC);
        assertThat(air.getNoise().getAverage()).isEqualTo(TEST_NOISE);
    }

    @Test
    void shouldAccumulatePrecipitationMetrics_whenSensorReadingIsPrecipitation() {
        TelemetryAnalysisAccumulator accumulator = new TelemetryAnalysisAccumulator();

        PrecipitationReading precipitationReading = Mockito.mock(PrecipitationReading.class);
        when(precipitationReading.getRainRateMmH()).thenReturn(TEST_RAIN);
        when(precipitationReading.getSnowDepthCm()).thenReturn(TEST_SNOW);
        when(precipitationReading.getEvaporationRate()).thenReturn(TEST_EVAPORATE);

        SensorReading proto = Mockito.mock(SensorReading.class);
        when(proto.getPrecipitation()).thenReturn(precipitationReading);

        accumulator.accumulate(portReading(PortSensorDataCase.PRECIPITATION, proto));

        PrecipitationAccumulator precip = groupOf(accumulator, PrecipitationAccumulator.class);
        assertThat(precip.getRain().getAverage()).isEqualTo(TEST_RAIN);
        assertThat(precip.getSnow().getAverage()).isEqualTo(TEST_SNOW);
        assertThat(precip.getEvaporate().getAverage()).isEqualTo(TEST_EVAPORATE);
    }

    @Test
    void shouldAccumulateOpticalMetrics_whenSensorReadingIsOptical() {
        TelemetryAnalysisAccumulator accumulator = new TelemetryAnalysisAccumulator();

        OpticalReading opticalReading = Mockito.mock(OpticalReading.class);
        when(opticalReading.getUvIndex()).thenReturn(TEST_UV);
        when(opticalReading.getSolarRadiationWm2()).thenReturn(TEST_SOLAR);
        when(opticalReading.getLux()).thenReturn(TEST_LUX);
        when(opticalReading.getVisibilityM()).thenReturn(TEST_VISIBILITY);

        SensorReading proto = Mockito.mock(SensorReading.class);
        when(proto.getOptical()).thenReturn(opticalReading);

        accumulator.accumulate(portReading(PortSensorDataCase.OPTICAL, proto));

        OpticalAccumulator optical = groupOf(accumulator, OpticalAccumulator.class);
        assertThat(optical.getUv().getAverage()).isEqualTo(TEST_UV);
        assertThat(optical.getSolar().getAverage()).isEqualTo(TEST_SOLAR);
        assertThat(optical.getLux().getAverage()).isEqualTo(TEST_LUX);
        assertThat(optical.getVis().getAverage()).isEqualTo(TEST_VISIBILITY);
    }

    @Test
    void shouldMergeAmbientStatistics_whenCombiningWithAnotherAccumulator() {
        TelemetryAnalysisAccumulator base = new TelemetryAnalysisAccumulator();
        TelemetryAnalysisAccumulator secondary = new TelemetryAnalysisAccumulator();

        groupOf(base, AmbientAccumulator.class).getTemp().accept(10.0);
        groupOf(secondary, AmbientAccumulator.class).getTemp().accept(20.0);

        base.merge(secondary);

        var temp = groupOf(base, AmbientAccumulator.class).getTemp();
        assertThat(temp.getCount()).isEqualTo(2);
        assertThat(temp.getAverage()).isEqualTo(15.0);
    }

    @Test
    void shouldMergeEveryGroupIndependently_whenCombiningAccumulatorsWithMultipleGroups() {
        TelemetryAnalysisAccumulator base = new TelemetryAnalysisAccumulator();
        TelemetryAnalysisAccumulator secondary = new TelemetryAnalysisAccumulator();

        groupOf(base, AmbientAccumulator.class).getTemp().accept(10.0);
        groupOf(base, WindAccumulator.class).getWindSpeed().accept(1.0);
        groupOf(secondary, AmbientAccumulator.class).getTemp().accept(20.0);
        groupOf(secondary, WindAccumulator.class).getWindSpeed().accept(3.0);

        base.merge(secondary);

        assertThat(groupOf(base, AmbientAccumulator.class).getTemp().getAverage()).isEqualTo(15.0);
        assertThat(groupOf(base, WindAccumulator.class).getWindSpeed().getAverage()).isEqualTo(2.0);
    }

    @Test
    void shouldMapAmbientAverages_whenLayerCountsAreGreaterThanZero() {
        TelemetryAnalysisAccumulator accumulator = new TelemetryAnalysisAccumulator();
        PortGridCellLayersBuilder builder = Mockito.mock(PortGridCellLayersBuilder.class);

        AmbientAccumulator ambient = groupOf(accumulator, AmbientAccumulator.class);
        ambient.getTemp().accept(TEST_TEMP);
        ambient.getHumidity().accept(TEST_HUMIDITY);
        ambient.getPressure().accept(TEST_PRESSURE);
        ambient.getLeafWetness().accept(TEST_LEAF_WETNESS);

        accumulator.applyTo(builder);

        verify(builder).setAvgTemperature(TEST_TEMP);
        verify(builder).setAvgHumidity(TEST_HUMIDITY);
        verify(builder).setAvgPressure(TEST_PRESSURE);
        verify(builder).setAvgLeafWetnessPct(TEST_LEAF_WETNESS);
    }

    @Test
    void shouldMapWindVectorToDirection_whenWindVectorIsProcessed() {
        TelemetryAnalysisAccumulator accumulator = new TelemetryAnalysisAccumulator();
        PortGridCellLayersBuilder builder = Mockito.mock(PortGridCellLayersBuilder.class);

        WindAccumulator wind = groupOf(accumulator, WindAccumulator.class);
        wind.getWindSpeed().accept(10.0);
        wind.getWindSin().accept(0.0);
        wind.getWindCos().accept(-1.0);

        accumulator.applyTo(builder);

        verify(builder).setAvgWindSpeed(10.0);
        verify(builder).setAvgWindDirection(180);
    }

    @Test
    void shouldMapAirQualityAverages_whenLayerCountsAreGreaterThanZero() {
        TelemetryAnalysisAccumulator accumulator = new TelemetryAnalysisAccumulator();
        PortGridCellLayersBuilder builder = Mockito.mock(PortGridCellLayersBuilder.class);

        AirQualityAccumulator air = groupOf(accumulator, AirQualityAccumulator.class);
        air.getPm100().accept(TEST_PM100);
        air.getPm25().accept(TEST_PM25);
        air.getPm10().accept(TEST_PM10);
        air.getVoc().accept(TEST_VOC);
        air.getNoise().accept(TEST_NOISE);

        accumulator.applyTo(builder);

        verify(builder).setAvgPm100(TEST_PM100);
        verify(builder).setAvgPm25(TEST_PM25);
        verify(builder).setAvgPm10(TEST_PM10);
        verify(builder).setAvgVoc(TEST_VOC);
        verify(builder).setAvgNoiseDb(TEST_NOISE);
    }

    @Test
    void shouldMapPrecipitationAverages_whenLayerCountsAreGreaterThanZero() {
        TelemetryAnalysisAccumulator accumulator = new TelemetryAnalysisAccumulator();
        PortGridCellLayersBuilder builder = Mockito.mock(PortGridCellLayersBuilder.class);

        PrecipitationAccumulator precip = groupOf(accumulator, PrecipitationAccumulator.class);
        precip.getRain().accept(TEST_RAIN);
        precip.getSnow().accept(TEST_SNOW);
        precip.getEvaporate().accept(TEST_EVAPORATE);

        accumulator.applyTo(builder);

        verify(builder).setAvgRainMm(TEST_RAIN);
        verify(builder).setAvgSnowCm(TEST_SNOW);
        verify(builder).setAvgEvapRate(TEST_EVAPORATE);
    }

    @Test
    void shouldMapOpticalAverages_whenLayerCountsAreGreaterThanZero() {
        TelemetryAnalysisAccumulator accumulator = new TelemetryAnalysisAccumulator();
        PortGridCellLayersBuilder builder = Mockito.mock(PortGridCellLayersBuilder.class);

        OpticalAccumulator optical = groupOf(accumulator, OpticalAccumulator.class);
        optical.getUv().accept(TEST_UV);
        optical.getSolar().accept(TEST_SOLAR);
        optical.getLux().accept(TEST_LUX);
        optical.getVis().accept(TEST_VISIBILITY);

        accumulator.applyTo(builder);

        verify(builder).setAvgUvIndex(TEST_UV);
        verify(builder).setAvgSolarRadiationWm2(TEST_SOLAR);
        verify(builder).setAvgLux(TEST_LUX);
        verify(builder).setAvgVisibilityM(TEST_VISIBILITY);
    }

    @Test
    void shouldAdd360ToDegrees_whenAtan2ReturnsNegativeAngle() {
        TelemetryAnalysisAccumulator accumulator = new TelemetryAnalysisAccumulator();
        PortGridCellLayersBuilder builder = Mockito.mock(PortGridCellLayersBuilder.class);

        WindAccumulator wind = groupOf(accumulator, WindAccumulator.class);
        wind.getWindSpeed().accept(10.0);
        wind.getWindSin().accept(-0.5);
        wind.getWindCos().accept(0.866);

        accumulator.applyTo(builder);

        verify(builder).setAvgWindDirection(330);
    }

    @Test
    void shouldDoNothingToStatistics_whenSensorReadingIsSensorDataNotSet() {
        TelemetryAnalysisAccumulator accumulator = new TelemetryAnalysisAccumulator();

        SensorReading proto = Mockito.mock(SensorReading.class);
        PortSensorReading reading = portReading(PortSensorDataCase.SENSOR_DATA_NOT_SET, proto);

        accumulator.accumulate(reading);

        assertThat(groupOf(accumulator, AmbientAccumulator.class).getTemp().getCount()).isZero();
        assertThat(groupOf(accumulator, WindAccumulator.class).getWindSpeed().getCount()).isZero();
    }

    @Test
    void shouldSkipBuilderProperties_whenLayerCountsAreZero() {
        TelemetryAnalysisAccumulator accumulator = new TelemetryAnalysisAccumulator();
        PortGridCellLayersBuilder builder = Mockito.mock(PortGridCellLayersBuilder.class);

        accumulator.applyTo(builder);

        verifyNoInteractions(builder);
    }

    @Test
    void shouldKeepStatisticsEmpty_whenNoReadingsAreAccumulated() {
        TelemetryAnalysisAccumulator accumulator = new TelemetryAnalysisAccumulator();

        assertThat(groupOf(accumulator, AmbientAccumulator.class).getTemp().getCount()).isZero();
        assertThat(groupOf(accumulator, AmbientAccumulator.class).getHumidity().getCount()).isZero();
        assertThat(groupOf(accumulator, WindAccumulator.class).getWindSpeed().getCount()).isZero();
        assertThat(groupOf(accumulator, AirQualityAccumulator.class).getPm100().getCount()).isZero();
        assertThat(groupOf(accumulator, PrecipitationAccumulator.class).getRain().getCount()).isZero();
        assertThat(groupOf(accumulator, OpticalAccumulator.class).getUv().getCount()).isZero();
    }

    @Test
    void shouldIgnoreAmbientStatistics_whenMergingWithEmptyAccumulator() {
        TelemetryAnalysisAccumulator base = new TelemetryAnalysisAccumulator();
        TelemetryAnalysisAccumulator empty = new TelemetryAnalysisAccumulator();

        groupOf(base, AmbientAccumulator.class).getTemp().accept(10.0);

        base.merge(empty);

        assertThat(groupOf(base, AmbientAccumulator.class).getTemp().getCount()).isEqualTo(1);
        assertThat(groupOf(base, AmbientAccumulator.class).getTemp().getAverage()).isEqualTo(10.0);
    }
}