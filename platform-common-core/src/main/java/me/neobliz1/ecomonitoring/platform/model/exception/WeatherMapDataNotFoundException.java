package me.neobliz1.ecomonitoring.platform.model.exception;

import static me.neobliz1.ecomonitoring.platform.model.exception.EcoPlatformErrorCode.WEATHER_MAP_DATA_NOT_FOUND;

public class WeatherMapDataNotFoundException extends BasePlatformException {

    public WeatherMapDataNotFoundException() {
        super(WEATHER_MAP_DATA_NOT_FOUND);
    }
}