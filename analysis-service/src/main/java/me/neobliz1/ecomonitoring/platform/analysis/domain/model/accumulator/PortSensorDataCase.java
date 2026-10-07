package me.neobliz1.ecomonitoring.platform.analysis.domain.model.accumulator;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public enum PortSensorDataCase {

    WIND(1),
    AMBIENT(2),
    AIR_QUALITY(3),
    PRECIPITATION(4),
    OPTICAL(5),
    SENSOR_DATA_NOT_SET(0);

    private final int value;

    public static PortSensorDataCase forNumber(int value) {
        return switch(value) {
            case 1 -> WIND;
            case 2 -> AMBIENT;
            case 3 -> AIR_QUALITY;
            case 4 -> PRECIPITATION;
            case 5 -> OPTICAL;
            case 0 -> SENSOR_DATA_NOT_SET;
            default -> null;
        };
    }
}
