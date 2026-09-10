package me.neobliz1.ecomonitoring.platform.model.dto;

import io.swagger.v3.oas.annotations.media.Schema;

@Schema(name = "ErrorEnvelopeDto")
public record ErrorEnvelopeDto(
        @Schema(example = "ERR_404_WEATHER_MAP_NOT_FOUND") String errorCode,
        @Schema(example = "Weather map not found for given coordinates") String errorDescription,
        @Schema(example = "1718845200123") long timestamp
) {
}
