package me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.inbound.web.doc;

import io.swagger.v3.oas.annotations.media.Schema;

@Schema(name = "GridCellLayers")
public record GridCellLayersDoc(
        @Schema(example = "v1234") String geohash,
        @Schema(example = "12") int readingCount,
        @Schema(example = "22.5") double avgTemperature,
        @Schema(example = "45.2") double avgHumidity,
        @Schema(example = "1013.2") double avgPressure,
        @Schema(example = "15.4") double avgLeafWetnessPct,
        @Schema(example = "3.6") double avgWindSpeed,
        @Schema(example = "180.0") double avgWindDirection,
        @Schema(example = "12.4") double avgPm25,
        @Schema(example = "24.8") double avgPm10,
        @Schema(example = "45.1") double avgPm100,
        @Schema(example = "0.35") double avgVoc,
        @Schema(example = "42.1") double avgNoiseDb,
        @Schema(example = "0.0") double avgRainMm,
        @Schema(example = "0.0") double avgSnowCm,
        @Schema(example = "1.2") double avgEvapRate,
        @Schema(example = "3.1") double avgUvIndex,
        @Schema(example = "345.5") double avgSolarRadiationWm2,
        @Schema(example = "12500.0") double avgLux,
        @Schema(example = "10000.0") double avgVisibilityM
) {
}
