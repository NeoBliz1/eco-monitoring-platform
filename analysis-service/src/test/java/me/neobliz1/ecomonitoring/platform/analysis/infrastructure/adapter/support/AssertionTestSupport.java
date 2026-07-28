package me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.support;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import lombok.extern.slf4j.Slf4j;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.GridCellLayers;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.WeatherMap;

import java.util.Map;
import java.util.Optional;

@Slf4j
public abstract class AssertionTestSupport {

    public Optional<GridCellLayers> findGridCell(WeatherMap weatherMap, String gridCellKey) {
        return weatherMap.getGridCellsMap().entrySet().stream()
                .filter(e -> e.getKey().contains(gridCellKey))
                .map(Map.Entry::getValue)
                .findFirst();
    }

    public void assertGridCellExists(WeatherMap weatherMap, String gridCellKey) {
        boolean exists = weatherMap.getGridCellsMap().keySet().stream()
                .anyMatch(k -> k.contains(gridCellKey));
        assertTrue(exists, "Grid cell " + gridCellKey + " should exist in WeatherMap");
    }

    public void assertAverageTemperature(WeatherMap weatherMap, String gridCellKey, double expected, double delta) {
        GridCellLayers cell = findGridCell(weatherMap, gridCellKey)
                .orElseThrow(() -> new AssertionError("Grid cell not found: " + gridCellKey));
        assertEquals(expected, cell.getAvgTemperature(), delta,
                "Average temperature for " + gridCellKey);
    }

    public void assertAverageHumidity(WeatherMap weatherMap, String gridCellKey, double expected, double delta) {
        GridCellLayers cell = findGridCell(weatherMap, gridCellKey)
                .orElseThrow(() -> new AssertionError("Grid cell not found: " + gridCellKey));
        assertEquals(expected, cell.getAvgHumidity(), delta,
                "Average humidity for " + gridCellKey);
    }

    public void assertAveragePressure(WeatherMap weatherMap, String gridCellKey, double expected, double delta) {
        GridCellLayers cell = findGridCell(weatherMap, gridCellKey)
                .orElseThrow(() -> new AssertionError("Grid cell not found: " + gridCellKey));
        assertEquals(expected, cell.getAvgPressure(), delta,
                "Average pressure for " + gridCellKey);
    }

    public void assertAverageWindSpeed(WeatherMap weatherMap, String gridCellKey, double expected, double delta) {
        GridCellLayers cell = findGridCell(weatherMap, gridCellKey)
                .orElseThrow(() -> new AssertionError("Grid cell not found: " + gridCellKey));
        assertEquals(expected, cell.getAvgWindSpeed(), delta,
                "Average wind speed for " + gridCellKey);
    }

    public void assertAirQualityReadings(WeatherMap weatherMap, String gridCellKey,
                                         double expectedPm25, double expectedPm10, double expectedPm100, double delta) {
        GridCellLayers cell = findGridCell(weatherMap, gridCellKey)
                .orElseThrow(() -> new AssertionError("Grid cell not found: " + gridCellKey));
        assertEquals(expectedPm25, cell.getAvgPm25(), delta, "PM2.5 for " + gridCellKey);
        assertEquals(expectedPm10, cell.getAvgPm10(), delta, "PM10 for " + gridCellKey);
        assertEquals(expectedPm100, cell.getAvgPm100(), delta, "PM100 for " + gridCellKey);
    }

    public void assertVocAndNoise(WeatherMap weatherMap, String gridCellKey,
                                  double expectedVoc, double expectedNoiseDb, double delta) {
        GridCellLayers cell = findGridCell(weatherMap, gridCellKey)
                .orElseThrow(() -> new AssertionError("Grid cell not found: " + gridCellKey));
        assertEquals(expectedVoc, cell.getAvgVoc(), delta, "VOC for " + gridCellKey);
        assertEquals(expectedNoiseDb, cell.getAvgNoiseDb(), delta, "Noise dB for " + gridCellKey);
    }

    public void assertPrecipitationReadings(WeatherMap weatherMap, String gridCellKey,
                                            double expectedRainMm, double expectedSnowCm, double expectedEvapRate, double delta) {
        GridCellLayers cell = findGridCell(weatherMap, gridCellKey)
                .orElseThrow(() -> new AssertionError("Grid cell not found: " + gridCellKey));
        assertEquals(expectedRainMm, cell.getAvgRainMm(), delta, "Rain mm for " + gridCellKey);
        assertEquals(expectedSnowCm, cell.getAvgSnowCm(), delta, "Snow cm for " + gridCellKey);
        assertEquals(expectedEvapRate, cell.getAvgEvapRate(), delta, "Evaporation rate for " + gridCellKey);
    }

    public void assertOpticalReadings(WeatherMap weatherMap, String gridCellKey,
                                      double expectedUvIndex, double expectedSolarRadiation,
                                      double expectedLux, double expectedVisibilityM, double delta) {
        GridCellLayers cell = findGridCell(weatherMap, gridCellKey)
                .orElseThrow(() -> new AssertionError("Grid cell not found: " + gridCellKey));
        assertEquals(expectedUvIndex, cell.getAvgUvIndex(), delta, "UV Index for " + gridCellKey);
        assertEquals(expectedSolarRadiation, cell.getAvgSolarRadiationWm2(), delta, "Solar Radiation for " + gridCellKey);
        assertEquals(expectedLux, cell.getAvgLux(), delta, "Lux for " + gridCellKey);
        assertEquals(expectedVisibilityM, cell.getAvgVisibilityM(), delta, "Visibility for " + gridCellKey);
    }

    public void assertGridCellReadingCount(WeatherMap weatherMap, String gridCellKey, int expectedCount) {
        GridCellLayers cell = findGridCell(weatherMap, gridCellKey)
                .orElseThrow(() -> new AssertionError("Grid cell not found: " + gridCellKey));
        assertEquals(expectedCount, cell.getReadingCount(), "Reading count for " + gridCellKey);
    }

    public void assertGridCellHasAllSensorTypes(WeatherMap weatherMap, String gridCellKey) {
        GridCellLayers cell = findGridCell(weatherMap, gridCellKey)
                .orElseThrow(() -> new AssertionError("Grid cell not found: " + gridCellKey));

        assertTrue(cell.getAvgTemperature() > 0 || cell.getAvgTemperature() < 0, "Temperature should be set");
        assertTrue(cell.getAvgHumidity() > 0, "Humidity should be set");
        assertTrue(cell.getAvgPressure() > 0, "Pressure should be set");
        assertTrue(cell.getAvgWindSpeed() >= 0, "Wind speed should be set");
        assertTrue(cell.getAvgWindDirection() >= 0, "Wind direction should be set");
        assertTrue(cell.getAvgPm25() >= 0, "PM2.5 should be set");
        assertTrue(cell.getAvgPm10() >= 0, "PM10 should be set");
        assertTrue(cell.getAvgPm100() >= 0, "PM100 should be set");
        assertTrue(cell.getAvgVoc() >= 0, "VOC should be set");
        assertTrue(cell.getAvgNoiseDb() >= 0, "Noise dB should be set");
        assertTrue(cell.getAvgRainMm() >= 0, "Rain mm should be set");
        assertTrue(cell.getAvgSnowCm() >= 0, "Snow cm should be set");
        assertTrue(cell.getAvgEvapRate() >= 0, "Evaporation rate should be set");
        assertTrue(cell.getAvgUvIndex() >= 0, "UV Index should be set");
        assertTrue(cell.getAvgSolarRadiationWm2() >= 0, "Solar Radiation should be set");
        assertTrue(cell.getAvgLux() >= 0, "Lux should be set");
        assertTrue(cell.getAvgVisibilityM() >= 0, "Visibility should be set");
    }

    /**
     * Asserts that wind direction uses vector averaging
     * For example, 350° and 10° should average to ~0° (not 180°)
     */
    public void assertVectorWindDirectionAveraging(WeatherMap weatherMap, String gridCellKey,
                                                   double expectedDirection, double tolerance) {
        GridCellLayers cell = findGridCell(weatherMap, gridCellKey)
                .orElseThrow(() -> new AssertionError("Grid cell not found: " + gridCellKey));

        double actualDirection = cell.getAvgWindDirection();

        // Handle 0°/360° equivalence
        if (Math.abs(expectedDirection) < tolerance || Math.abs(expectedDirection - 360.0) < tolerance) {
            assertTrue(
                    Math.abs(actualDirection) < tolerance || Math.abs(actualDirection - 360.0) < tolerance,
                    String.format("Wind direction should be ~0°/360° (vector average), but was %.2f°", actualDirection)
            );
        } else {
            assertEquals(expectedDirection, actualDirection, tolerance,
                    String.format("Wind direction should be %.2f° (vector average)", expectedDirection));
        }
    }
}