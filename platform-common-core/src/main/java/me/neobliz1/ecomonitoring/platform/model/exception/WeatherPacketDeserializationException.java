package me.neobliz1.ecomonitoring.platform.model.exception;

import static me.neobliz1.ecomonitoring.platform.model.exception.EcoPlatformErrorCode.WEATHER_PACKET_DESERIALIZATION_ERROR;

public class WeatherPacketDeserializationException extends BasePlatformException {

    public WeatherPacketDeserializationException(Throwable cause) {
        super(WEATHER_PACKET_DESERIALIZATION_ERROR);
        initCause(cause);
    }
}
