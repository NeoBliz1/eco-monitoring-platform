package me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.inbound.web.doc;

import io.swagger.v3.oas.annotations.media.Schema;

import java.util.Map;

@Schema(name = "WeatherMapResponse")
public record WeatherMapResponse(
        @Schema(example = "1718845200000") long timestampBucket,
        @Schema(example = "15") int intervalMinutes,
        @Schema(example = "{\"v1234\": {\"geohash\": \"v1234\", \"reading_count\": 12, \"avg_temperature\": 22.5}}")
        Map<String, GridCellLayersDoc> gridCells
) {
}