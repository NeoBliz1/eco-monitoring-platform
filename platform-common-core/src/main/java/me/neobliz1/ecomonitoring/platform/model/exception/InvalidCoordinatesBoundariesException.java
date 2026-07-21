package me.neobliz1.ecomonitoring.platform.model.exception;

import static me.neobliz1.ecomonitoring.platform.model.exception.EcoPlatformErrorCode.INVALID_COORDINATES_BOUNDARIES;

public class InvalidCoordinatesBoundariesException extends BasePlatformException {

    public InvalidCoordinatesBoundariesException() {
        super(INVALID_COORDINATES_BOUNDARIES);
    }
}