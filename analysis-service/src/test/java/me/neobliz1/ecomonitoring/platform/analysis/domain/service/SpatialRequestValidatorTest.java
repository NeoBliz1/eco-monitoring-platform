package me.neobliz1.ecomonitoring.platform.analysis.domain.service;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import me.neobliz1.ecomonitoring.platform.model.exception.CoordinatesSquareTooLargeException;
import me.neobliz1.ecomonitoring.platform.model.exception.InvalidCoordinatesBoundariesException;
import me.neobliz1.ecomonitoring.platform.model.exception.InvalidCoordinatesSquareException;
import org.junit.jupiter.api.Test;

class SpatialRequestValidatorTest {

    private static final Double VALID_MIN_LAT = 50.0;
    private static final Double VALID_MAX_LAT = 55.0;
    private static final Double VALID_MIN_LON = 30.0;
    private static final Double VALID_MAX_LON = 35.0;
    private static final double MAX_SPAN = 5.0;

    @Test
    void shouldNotThrowException_whenValidCoordinatesProvided() {
        SpatialRequestValidator.validateCoordinatesBox(VALID_MIN_LAT, VALID_MAX_LAT, VALID_MIN_LON, VALID_MAX_LON);
    }

    @Test
    void shouldNotThrowException_whenCoordinatesAreNegativeValidValues() {
        SpatialRequestValidator.validateCoordinatesBox(-50.0, -45.0, -130.0, -125.0);
    }

    @Test
    void shouldNotThrowException_whenCoordinatesAtExactBoundaryValues() {
        SpatialRequestValidator.validateCoordinatesBox(-90.0, -85.0, -180.0, -175.0);
    }

    @Test
    void shouldNotThrowException_whenLatitudeSpanEqualsFive() {
        SpatialRequestValidator.validateCoordinatesBox(50.0, 55.0, 30.0, 31.0);
    }

    @Test
    void shouldNotThrowException_whenLongitudeSpanEqualsFive() {
        SpatialRequestValidator.validateCoordinatesBox(50.0, 51.0, 30.0, 35.0);
    }

    @Test
    void shouldThrowInvalidCoordinatesBoundariesException_whenMinLatBelowNegativeNinety() {
        assertThatThrownBy(() -> SpatialRequestValidator.validateCoordinatesBox(-91.0, VALID_MAX_LAT, VALID_MIN_LON, VALID_MAX_LON))
                .isInstanceOf(InvalidCoordinatesBoundariesException.class);
    }

    @Test
    void shouldThrowInvalidCoordinatesBoundariesException_whenMaxLatAboveNinety() {
        assertThatThrownBy(() -> SpatialRequestValidator.validateCoordinatesBox(VALID_MIN_LAT, 91.0, VALID_MIN_LON, VALID_MAX_LON))
                .isInstanceOf(InvalidCoordinatesBoundariesException.class);
    }

    @Test
    void shouldThrowInvalidCoordinatesBoundariesException_whenMinLonBelowNegativeOneEighty() {
        assertThatThrownBy(() -> SpatialRequestValidator.validateCoordinatesBox(VALID_MIN_LAT, VALID_MAX_LAT, -181.0, VALID_MAX_LON))
                .isInstanceOf(InvalidCoordinatesBoundariesException.class);
    }

    @Test
    void shouldThrowInvalidCoordinatesBoundariesException_whenMaxLonAboveOneEighty() {
        assertThatThrownBy(() -> SpatialRequestValidator.validateCoordinatesBox(VALID_MIN_LAT, VALID_MAX_LAT, VALID_MIN_LON, 181.0))
                .isInstanceOf(InvalidCoordinatesBoundariesException.class);
    }

    @Test
    void shouldThrowInvalidCoordinatesSquareException_whenMaxLatNotGreaterThanMinLat() {
        assertThatThrownBy(() -> SpatialRequestValidator.validateCoordinatesBox(55.0, 50.0, VALID_MIN_LON, VALID_MAX_LON))
                .isInstanceOf(InvalidCoordinatesSquareException.class);
    }

    @Test
    void shouldThrowInvalidCoordinatesSquareException_whenMaxLonNotGreaterThanMinLon() {
        assertThatThrownBy(() -> SpatialRequestValidator.validateCoordinatesBox(VALID_MIN_LAT, VALID_MAX_LAT, 35.0, 30.0))
                .isInstanceOf(InvalidCoordinatesSquareException.class);
    }

    @Test
    void shouldThrowInvalidCoordinatesSquareException_whenLatitudesEqual() {
        assertThatThrownBy(() -> SpatialRequestValidator.validateCoordinatesBox(50.0, 50.0, VALID_MIN_LON, VALID_MAX_LON))
                .isInstanceOf(InvalidCoordinatesSquareException.class);
    }

    @Test
    void shouldThrowInvalidCoordinatesSquareException_whenLongitudesEqual() {
        assertThatThrownBy(() -> SpatialRequestValidator.validateCoordinatesBox(VALID_MIN_LAT, VALID_MAX_LAT, 30.0, 30.0))
                .isInstanceOf(InvalidCoordinatesSquareException.class);
    }

    @Test
    void shouldThrowCoordinatesSquareTooLargeException_whenLatitudeSpanExceedsFive() {
        assertThatThrownBy(() -> SpatialRequestValidator.validateCoordinatesBox(VALID_MIN_LAT, VALID_MIN_LAT+MAX_SPAN+0.1, VALID_MIN_LON, VALID_MIN_LON+1.0))
                .isInstanceOf(CoordinatesSquareTooLargeException.class);
    }

    @Test
    void shouldThrowCoordinatesSquareTooLargeException_whenLongitudeSpanExceedsFive() {
        assertThatThrownBy(() -> SpatialRequestValidator.validateCoordinatesBox(VALID_MIN_LAT, VALID_MIN_LAT+1.0, VALID_MIN_LON, VALID_MIN_LON+MAX_SPAN+0.1))
                .isInstanceOf(CoordinatesSquareTooLargeException.class);
    }

    @Test
    void shouldThrowCoordinatesSquareTooLargeException_whenBothSpansExceedFive() {
        assertThatThrownBy(() -> SpatialRequestValidator.validateCoordinatesBox(VALID_MIN_LAT, VALID_MIN_LAT+MAX_SPAN+0.5, VALID_MIN_LON, VALID_MIN_LON+MAX_SPAN+0.5))
                .isInstanceOf(CoordinatesSquareTooLargeException.class);
    }
}
