package me.neobliz1.ecomonitoring.platform.analysis.domain.model.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lombok.With;

import java.beans.ConstructorProperties;

public record WeatherMapAnalysisRequestQuery(
        @With
        @Min(value = 0L, message = "Timestamp cannot be negative")
        @Max(value = 4102444800000L, message = "Timestamp cannot be unreasonably far in the future (Max: Year 2100)")
        long targetTimestamp,
        @NotNull(message = "Minimum latitude is required")
        Double minLat,
        @NotNull(message = "Maximum latitude is required")
        Double maxLat,
        @NotNull(message = "Minimum longitude is required")
        Double minLon,
        @NotNull(message = "Maximum longitude is required")
        Double maxLon
) {

    public static final String MIN_LAT = "min-lat";
    public static final String MAX_LAT = "max-lat";
    public static final String MIN_LON = "min-lon";
    public static final String MAX_LON = "max-lon";
    public static final String TARGET_TIMESTAMP = "target-timestamp";

    @ConstructorProperties({ TARGET_TIMESTAMP, MIN_LAT, MAX_LAT, MIN_LON, MAX_LON })
    public WeatherMapAnalysisRequestQuery {
    }
}
