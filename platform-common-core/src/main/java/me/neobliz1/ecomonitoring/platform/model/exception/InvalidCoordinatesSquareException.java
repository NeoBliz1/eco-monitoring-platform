package me.neobliz1.ecomonitoring.platform.model.exception;

import static me.neobliz1.ecomonitoring.platform.model.exception.EcoPlatformErrorCode.INVALID_COORDINATES_SQUARE;

public class InvalidCoordinatesSquareException extends BasePlatformException {

    public InvalidCoordinatesSquareException() {
        super(INVALID_COORDINATES_SQUARE);
    }
}