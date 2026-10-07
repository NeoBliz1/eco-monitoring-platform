package me.neobliz1.ecomonitoring.platform.history.infrastructure.mapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import me.neobliz1.ecomonitoring.platform.history.domain.model.entity.WeatherGridCellLayer;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.GridCellLayers;
import org.junit.jupiter.api.Test;

class GridCellLayerAggregatorTest {

    private static final double DELTA = 1e-9;

    @Test
    void shouldComputeWeightedAverage_whenBothCountsArePositive() {
        WeatherGridCellLayer existing = new WeatherGridCellLayer();
        existing.setReadingCount(3);
        existing.setAvgTemperature(10.0);
        GridCellLayers incoming = GridCellLayers.newBuilder()
                .setReadingCount(1)
                .setAvgTemperature(30.0)
                .build();
        GridCellLayerAggregator aggregator = new GridCellLayerAggregator();

        aggregator.mergeNewGridCellLayerIntoExistingGridCellLayer(incoming, existing);

        assertThat(existing.getAvgTemperature()).isCloseTo(15.0, within(DELTA));
        assertThat(existing.getReadingCount()).isEqualTo(4);
    }

    @Test
    void shouldReturnNewValue_whenOldValueIsNull() {
        WeatherGridCellLayer existing = new WeatherGridCellLayer();
        existing.setReadingCount(3);
        GridCellLayers incoming = GridCellLayers.newBuilder()
                .setReadingCount(1)
                .setAvgTemperature(30.0)
                .build();
        GridCellLayerAggregator aggregator = new GridCellLayerAggregator();

        aggregator.mergeNewGridCellLayerIntoExistingGridCellLayer(incoming, existing);

        assertThat(existing.getAvgTemperature()).isEqualTo(30.0);
    }

    @Test
    void shouldReturnNewValue_whenOldCountIsZero() {
        WeatherGridCellLayer existing = new WeatherGridCellLayer();
        existing.setReadingCount(0);
        existing.setAvgTemperature(10.0);
        GridCellLayers incoming = GridCellLayers.newBuilder()
                .setReadingCount(1)
                .setAvgTemperature(30.0)
                .build();
        GridCellLayerAggregator aggregator = new GridCellLayerAggregator();

        aggregator.mergeNewGridCellLayerIntoExistingGridCellLayer(incoming, existing);

        assertThat(existing.getAvgTemperature()).isEqualTo(30.0);
    }

    @Test
    void shouldKeepOldValue_whenNewCountIsZero() {
        WeatherGridCellLayer existing = new WeatherGridCellLayer();
        existing.setReadingCount(3);
        existing.setAvgTemperature(10.0);
        GridCellLayers incoming = GridCellLayers.newBuilder()
                .setReadingCount(0)
                .setAvgTemperature(30.0)
                .build();
        GridCellLayerAggregator aggregator = new GridCellLayerAggregator();

        aggregator.mergeNewGridCellLayerIntoExistingGridCellLayer(incoming, existing);

        assertThat(existing.getAvgTemperature()).isEqualTo(10.0);
    }

    @Test
    void shouldAccumulateReadingCount_whenMerging() {
        WeatherGridCellLayer existing = new WeatherGridCellLayer();
        existing.setReadingCount(5);
        GridCellLayers incoming = GridCellLayers.newBuilder()
                .setReadingCount(7)
                .build();
        GridCellLayerAggregator aggregator = new GridCellLayerAggregator();

        aggregator.mergeNewGridCellLayerIntoExistingGridCellLayer(incoming, existing);

        assertThat(existing.getReadingCount()).isEqualTo(12);
    }

    @Test
    void shouldMergeEveryField_whenAllValuesArePresent() {
        WeatherGridCellLayer existing = fullyPopulatedExisting();
        GridCellLayers incoming = fullyPopulatedIncoming();
        GridCellLayerAggregator aggregator = new GridCellLayerAggregator();

        aggregator.mergeNewGridCellLayerIntoExistingGridCellLayer(incoming, existing);

        assertThat(existing.getAvgTemperature()).isCloseTo(20.0, within(DELTA));
        assertThat(existing.getAvgHumidity()).isCloseTo(20.0, within(DELTA));
        assertThat(existing.getAvgPressure()).isCloseTo(20.0, within(DELTA));
        assertThat(existing.getAvgLeaf_wetnessPct()).isCloseTo(20.0, within(DELTA));
        assertThat(existing.getAvgWindSpeed()).isCloseTo(20.0, within(DELTA));
        assertThat(existing.getAvgWindDirection()).isCloseTo(20.0, within(DELTA));
        assertThat(existing.getAvgPm25()).isCloseTo(20.0, within(DELTA));
        assertThat(existing.getAvgPm10()).isCloseTo(20.0, within(DELTA));
        assertThat(existing.getAvgPm100()).isCloseTo(20.0, within(DELTA));
        assertThat(existing.getAvgVoc()).isCloseTo(20.0, within(DELTA));
        assertThat(existing.getAvgNoiseDb()).isCloseTo(20.0, within(DELTA));
        assertThat(existing.getAvgRainMm()).isCloseTo(20.0, within(DELTA));
        assertThat(existing.getAvgSnowCm()).isCloseTo(20.0, within(DELTA));
        assertThat(existing.getAvgEvapRate()).isCloseTo(20.0, within(DELTA));
        assertThat(existing.getAvgUvIndex()).isCloseTo(20.0, within(DELTA));
        assertThat(existing.getAvgSolarRadiationWm2()).isCloseTo(20.0, within(DELTA));
        assertThat(existing.getAvgLux()).isCloseTo(20.0, within(DELTA));
        assertThat(existing.getAvgVisibilityM()).isCloseTo(20.0, within(DELTA));
    }

    @Test
    void shouldBeReusable_whenMergingTwiceOnSameAggregatorInstance() {
        WeatherGridCellLayer existing = new WeatherGridCellLayer();
        existing.setReadingCount(0);
        GridCellLayerAggregator aggregator = new GridCellLayerAggregator();

        aggregator.mergeNewGridCellLayerIntoExistingGridCellLayer(
                GridCellLayers.newBuilder().setReadingCount(1).setAvgTemperature(10.0).build(), existing);
        aggregator.mergeNewGridCellLayerIntoExistingGridCellLayer(
                GridCellLayers.newBuilder().setReadingCount(1).setAvgTemperature(30.0).build(), existing);

        assertThat(existing.getReadingCount()).isEqualTo(2);
        assertThat(existing.getAvgTemperature()).isCloseTo(20.0, within(DELTA));
    }

    private WeatherGridCellLayer fullyPopulatedExisting() {
        WeatherGridCellLayer cell = new WeatherGridCellLayer();
        cell.setReadingCount(2);
        cell.setAvgTemperature(10.0);
        cell.setAvgHumidity(10.0);
        cell.setAvgPressure(10.0);
        cell.setAvgLeaf_wetnessPct(10.0);
        cell.setAvgWindSpeed(10.0);
        cell.setAvgWindDirection(10.0);
        cell.setAvgPm25(10.0);
        cell.setAvgPm10(10.0);
        cell.setAvgPm100(10.0);
        cell.setAvgVoc(10.0);
        cell.setAvgNoiseDb(10.0);
        cell.setAvgRainMm(10.0);
        cell.setAvgSnowCm(10.0);
        cell.setAvgEvapRate(10.0);
        cell.setAvgUvIndex(10.0);
        cell.setAvgSolarRadiationWm2(10.0);
        cell.setAvgLux(10.0);
        cell.setAvgVisibilityM(10.0);
        return cell;
    }

    private GridCellLayers fullyPopulatedIncoming() {
        return GridCellLayers.newBuilder()
                .setReadingCount(2)
                .setAvgTemperature(30.0)
                .setAvgHumidity(30.0)
                .setAvgPressure(30.0)
                .setAvgLeafWetnessPct(30.0)
                .setAvgWindSpeed(30.0)
                .setAvgWindDirection(30.0)
                .setAvgPm25(30.0)
                .setAvgPm10(30.0)
                .setAvgPm100(30.0)
                .setAvgVoc(30.0)
                .setAvgNoiseDb(30.0)
                .setAvgRainMm(30.0)
                .setAvgSnowCm(30.0)
                .setAvgEvapRate(30.0)
                .setAvgUvIndex(30.0)
                .setAvgSolarRadiationWm2(30.0)
                .setAvgLux(30.0)
                .setAvgVisibilityM(30.0)
                .build();
    }
}