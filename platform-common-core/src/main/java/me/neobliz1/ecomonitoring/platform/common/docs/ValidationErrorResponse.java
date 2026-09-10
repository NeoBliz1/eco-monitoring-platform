package me.neobliz1.ecomonitoring.platform.common.docs;

import io.swagger.v3.oas.annotations.media.Schema;

import java.util.List;

@Schema(name = "ValidationErrorResponse")
public record ValidationErrorResponse(
        @Schema(example = "400") int status,
        @Schema(example = "Bad Request") String error,
        @Schema(example = "[\"station_id: must match pattern ^[0-9]{11}$\"]") List<String> violations
) {
}