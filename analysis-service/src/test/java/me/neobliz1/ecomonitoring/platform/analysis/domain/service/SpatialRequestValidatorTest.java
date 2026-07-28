package me.neobliz1.ecomonitoring.platform.analysis.domain.service;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import me.neobliz1.ecomonitoring.platform.model.exception.CoordinatesSquareTooLargeException;
import me.neobliz1.ecomonitoring.platform.model.exception.InvalidCoordinatesBoundariesException;
import me.neobliz1.ecomonitoring.platform.model.exception.InvalidCoordinatesSquareException;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

class SpatialRequestValidatorTest {

    private static final Double VALID_MIN_LAT = 50.0;
    private static final Double VALID_MAX_LAT = 55.0;
    private static final Double VALID_MIN_LON = 30.0;
    private static final Double VALID_MAX_LON = 35.0;
    private static final List<Double> VALID_COORDINATES = Arrays.asList(
            VALID_MIN_LAT, VALID_MAX_LAT, VALID_MIN_LON, VALID_MAX_LON
    );

    private static final double BOUNDARY_LAT_MIN = -90.0;
    private static final double BOUNDARY_LON_MIN = -180.0;
    private static final double MAX_SPAN = 5.0;

    @Test
    @SuppressWarnings("DataFlowIssue")
    void shouldThrowInvalidCoordinatesSquareException_whenCoordinatesSquareIsNull() {
        assertThatThrownBy(() -> SpatialRequestValidator.validateCoordinatesBox(null))
                .isInstanceOf(InvalidCoordinatesSquareException.class);
    }

    @Test
    void shouldThrowInvalidCoordinatesSquareException_whenCoordinatesSquareIsEmpty() {
        assertThatThrownBy(() -> SpatialRequestValidator.validateCoordinatesBox(Collections.emptyList()))
                .isInstanceOf(InvalidCoordinatesSquareException.class);
    }

    @Test
    void shouldThrowInvalidCoordinatesSquareException_whenCoordinatesSquareHasLessThanFourElements() {
        assertThatThrownBy(() -> SpatialRequestValidator.validateCoordinatesBox(Arrays.asList(1.0, 2.0, 3.0)))
                .isInstanceOf(InvalidCoordinatesSquareException.class);
    }

    @Test
    void shouldThrowInvalidCoordinatesSquareException_whenCoordinatesSquareHasOnlyThreeElements() {
        assertThatThrownBy(() -> SpatialRequestValidator.validateCoordinatesBox(Arrays.asList(1.0, 2.0, 3.0)))
                .isInstanceOf(InvalidCoordinatesSquareException.class);
    }

    @Test
    void shouldNotThrowException_whenCoordinatesSquareIsValid() {
        SpatialRequestValidator.validateCoordinatesBox(VALID_COORDINATES);
    }

    @Test
    void shouldNotThrowException_whenCoordinatesSquareHasExactBoundaryValues() {
        List<Double> coordinates = Arrays.asList(BOUNDARY_LAT_MIN, BOUNDARY_LAT_MIN+MAX_SPAN, BOUNDARY_LON_MIN, BOUNDARY_LON_MIN+MAX_SPAN);

        SpatialRequestValidator.validateCoordinatesBox(coordinates);
    }

    @Test
    void shouldThrowInvalidCoordinatesBoundariesException_whenMinLatBelowNegativeNinety() {
        List<Double> coordinates = Arrays.asList(-91.0, VALID_MAX_LAT, VALID_MIN_LON, VALID_MAX_LON);

        assertThatThrownBy(() -> SpatialRequestValidator.validateCoordinatesBox(coordinates))
                .isInstanceOf(InvalidCoordinatesBoundariesException.class);
    }

    @Test
    void shouldThrowInvalidCoordinatesBoundariesException_whenMaxLatAboveNinety() {
        List<Double> coordinates = Arrays.asList(VALID_MIN_LAT, 91.0, VALID_MIN_LON, VALID_MAX_LON);

        assertThatThrownBy(() -> SpatialRequestValidator.validateCoordinatesBox(coordinates))
                .isInstanceOf(InvalidCoordinatesBoundariesException.class);
    }

    @Test
    void shouldThrowInvalidCoordinatesBoundariesException_whenMinLonBelowNegativeOneEighty() {
        List<Double> coordinates = Arrays.asList(VALID_MIN_LAT, VALID_MAX_LAT, -181.0, VALID_MAX_LON);

        assertThatThrownBy(() -> SpatialRequestValidator.validateCoordinatesBox(coordinates))
                .isInstanceOf(InvalidCoordinatesBoundariesException.class);
    }

    @Test
    void shouldThrowInvalidCoordinatesBoundariesException_whenMaxLonAboveOneEighty() {
        List<Double> coordinates = Arrays.asList(VALID_MIN_LAT, VALID_MAX_LAT, VALID_MIN_LON, 181.0);

        assertThatThrownBy(() -> SpatialRequestValidator.validateCoordinatesBox(coordinates))
                .isInstanceOf(InvalidCoordinatesBoundariesException.class);
    }

    @Test
    void shouldNotThrowException_whenLatitudeSpanEqualsFive() {
        List<Double> coordinates = Arrays.asList(VALID_MIN_LAT, VALID_MIN_LAT+MAX_SPAN, VALID_MIN_LON, VALID_MIN_LON+1.0);

        SpatialRequestValidator.validateCoordinatesBox(coordinates);
    }

    @Test
    void shouldNotThrowException_whenLongitudeSpanEqualsFive() {
        List<Double> coordinates = Arrays.asList(VALID_MIN_LAT, VALID_MIN_LAT+1.0, VALID_MIN_LON, VALID_MIN_LON+MAX_SPAN);

        SpatialRequestValidator.validateCoordinatesBox(coordinates);
    }

    @Test
    void shouldThrowCoordinatesSquareTooLargeException_whenLatitudeSpanExceedsFive() {
        List<Double> coordinates = Arrays.asList(VALID_MIN_LAT, VALID_MIN_LAT+MAX_SPAN+0.1, VALID_MIN_LON, VALID_MIN_LON+1.0);

        assertThatThrownBy(() -> SpatialRequestValidator.validateCoordinatesBox(coordinates))
                .isInstanceOf(CoordinatesSquareTooLargeException.class);
    }

    @Test
    void shouldThrowCoordinatesSquareTooLargeException_whenLongitudeSpanExceedsFive() {
        List<Double> coordinates = Arrays.asList(VALID_MIN_LAT, VALID_MIN_LAT+1.0, VALID_MIN_LON, VALID_MIN_LON+MAX_SPAN+0.1);

        assertThatThrownBy(() -> SpatialRequestValidator.validateCoordinatesBox(coordinates))
                .isInstanceOf(CoordinatesSquareTooLargeException.class);
    }

    @Test
    void shouldThrowCoordinatesSquareTooLargeException_whenBothSpansExceedFive() {
        List<Double> coordinates = Arrays.asList(VALID_MIN_LAT, VALID_MIN_LAT+MAX_SPAN+0.5, VALID_MIN_LON, VALID_MIN_LON+MAX_SPAN+0.5);

        assertThatThrownBy(() -> SpatialRequestValidator.validateCoordinatesBox(coordinates))
                .isInstanceOf(CoordinatesSquareTooLargeException.class);
    }

    @Test
    void shouldNotThrowException_whenCoordinatesAreNegativeValidValues() {
        List<Double> coordinates = Arrays.asList(-50.0, -45.0, -130.0, -125.0);

        SpatialRequestValidator.validateCoordinatesBox(coordinates);
    }

    @Test
    void shouldThrowInvalidCoordinatesSquareException_whenCoordinatesSquareHasExactlyTwoElements() {
        assertThatThrownBy(() -> SpatialRequestValidator.validateCoordinatesBox(Arrays.asList(1.0, 2.0)))
                .isInstanceOf(InvalidCoordinatesSquareException.class);
    }

    @Test
    void shouldThrowInvalidCoordinatesSquareException_whenCoordinatesSquareHasExactlyOneElement() {
        assertThatThrownBy(() -> SpatialRequestValidator.validateCoordinatesBox(List.of(1.0)))
                .isInstanceOf(InvalidCoordinatesSquareException.class);
    }
}
