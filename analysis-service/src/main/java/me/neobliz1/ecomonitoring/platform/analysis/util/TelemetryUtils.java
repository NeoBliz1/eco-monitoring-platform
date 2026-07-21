package me.neobliz1.ecomonitoring.platform.analysis.util;

import lombok.experimental.UtilityClass;

import java.time.Duration;

@UtilityClass
public class TelemetryUtils {

    public static long getMillis(long intervalSeconds) {
        return Duration.ofSeconds(intervalSeconds).toMillis();
    }
}