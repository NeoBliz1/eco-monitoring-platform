package me.neobliz1.ecomonitoring.platform.model.dto;

import io.swagger.v3.oas.annotations.media.Schema;

@Schema(name = "ErrorEnvelopeDto", description = "Standard global error envelope payload wrapper returned by the platform core engine")
public record ErrorEnvelopeDto(
        @Schema(description = "Platform-specific logical alphanumeric error tracking code or standard HTTP Status code string",
                example = "ERR_404_WEATHER_MAP_NOT_FOUND")
        String errorCode,

        @Schema(description = "Analysis detailing the root cause of the processed failure",
                example = "Requested geo-spatial coordinates matrix box contains no active monitoring cell reports.")
        String errorDescription,

        @Schema(description = "Epoch millisecond unix registration timestamp indicating exactly when the fault occurred", example = "1718845200123")
        long timestamp
) {
}
