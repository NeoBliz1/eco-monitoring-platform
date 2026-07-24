package me.neobliz1.ecomonitoring.platform.analysis.domain.service;

import lombok.experimental.UtilityClass;
import me.neobliz1.ecomonitoring.platform.model.exception.CoordinatesSquareTooLargeException;
import me.neobliz1.ecomonitoring.platform.model.exception.InvalidCoordinatesBoundariesException;
import me.neobliz1.ecomonitoring.platform.model.exception.InvalidCoordinatesSquareException;

import java.util.List;

@UtilityClass
public class SpatialRequestValidator {

    public static void validateCoordinatesBox(List<Double> coordinatesSquare) {
        if (coordinatesSquare == null || coordinatesSquare.size() < 4) {
            throw new InvalidCoordinatesSquareException();
        }

        double minLat = coordinatesSquare.get(0);
        double maxLat = coordinatesSquare.get(1);
        double minLon = coordinatesSquare.get(2);
        double maxLon = coordinatesSquare.get(3);

        if (minLat < -90.0 || maxLat > 90.0 || minLon < -180.0 || maxLon > 180.0) {
            throw new InvalidCoordinatesBoundariesException();
        }

        if (Math.abs(maxLat - minLat) > 5.0 || Math.abs(maxLon - minLon) > 5.0) {
            throw new CoordinatesSquareTooLargeException();
        }
    }
}