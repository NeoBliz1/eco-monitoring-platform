package me.neobliz1.ecomonitoring.platform.model.exception;

import static me.neobliz1.ecomonitoring.platform.model.exception.EcoPlatformErrorCode.COORDINATES_SQUARE_TOO_LARGE;

public class CoordinatesSquareTooLargeException extends BasePlatformException {

    public CoordinatesSquareTooLargeException() {
        super(COORDINATES_SQUARE_TOO_LARGE);
    }
}