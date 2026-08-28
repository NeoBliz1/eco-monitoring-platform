package me.neobliz1.ecomonitoring.platform.ingestion.infrastructure.adapter.inbound.web.docs;

import io.swagger.v3.oas.annotations.media.Schema;

import java.util.List;

@Schema(name = "ValidationErrorResponse", description = "Payload structure returned when incoming Protobuf packet validation constraints fail")
public record ValidationErrorResponse(
        @Schema(description = "HTTP status code matching the response context", example = "400")
        int status,

        @Schema(description = "Standard HTTP error reason keyword phrase", example = "Bad Request")
        String error,

        @Schema(
                description = "List collection detailing the exact field constraint violations verified by Buf Validate engine annotations",
                example = "[\"station_id: must match pattern ^[0-9]{11}$\", \"readings: must have at least 1 sensor reading\"]"
        )
        List<String> violations
) {
}