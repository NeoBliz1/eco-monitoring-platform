package me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.inbound.web.doc;

import io.swagger.v3.oas.annotations.media.Schema;

@Schema(
        name = "GridCellLayers",
        description = "Aggregated multi-sensor climatic logging values computed for a specific geospatial map cell coordinate"
)
public record GridCellLayersDoc(
        @Schema(name = "geohash", description = "Geospatial unique index hash identity code", example = "v1234")
        String geohash,

        @Schema(name = "reading_count", description = "Total collected telemetry payload slices parsed during the calculation", example = "12")
        int readingCount,

        @Schema(name = "avg_temperature", description = "Mean ambient thermal sensor reading output calibrated in Celsius degrees", example = "22.5")
        double avgTemperature,

        @Schema(name = "avg_humidity", description = "Calculated proportional environmental moisture content percentage value", example = "45.2")
        double avgHumidity,

        @Schema(name = "avg_pressure", description = "Mean calculated weight force atmospheric barometric pressure value registered in Hectopascals", example = "1013.2")
        double avgPressure,

        @Schema(name = "avg_leaf_wetness_pct", description = "Calculated average water film layer moisture value present on flora surfaces", example = "15.4")
        double avgLeafWetnessPct,

        @Schema(name = "avg_wind_speed", description = "Calculated mean air molecule displacement velocity over ground", example = "3.6")
        double avgWindSpeed,

        @Schema(name = "avg_wind_direction", description = "Mean compass heading directional angle calculation representing wind derivation source", example = "180.0")
        double avgWindDirection,

        @Schema(name = "avg_pm25", description = "Mean atmospheric mass concentration value of fine inhalable particulate matter under 2.5 micrometers", example = "12.4")
        double avgPm25,

        @Schema(name = "avg_pm10", description = "Mean atmospheric mass concentration value of inhalable particulate matter under 10 micrometers", example = "24.8")
        double avgPm10,

        @Schema(name = "avg_pm100", description = "Mean atmospheric mass concentration value of large suspended particulate matter under 100 micrometers", example = "45.1")
        double avgPm100,

        @Schema(name = "avg_voc", description = "Mean aggregate sensor concentration level tracking harmful Volatile Organic Compounds", example = "0.35")
        double avgVoc,

        @Schema(name = "avg_noise_db", description = "Calculated equivalent ambient acoustic displacement pressure tracking environment loudness", example = "42.1")
        double avgNoiseDb,

        @Schema(name = "avg_rain_mm", description = "Accumulated liquid precipitation water thickness layer index", example = "0.0")
        double avgRainMm,

        @Schema(name = "avg_snow_cm", description = "Accumulated solid frozen precipitation structure physical depth measurement", example = "0.0")
        double avgSnowCm,

        @Schema(name = "avg_evap_rate", description = "Calculated moisture displacement transform volume converted from liquid to gas state profiles", example = "1.2")
        double avgEvapRate,

        @Schema(name = "avg_uv_index", description = "Calculated standard solar ultraviolet radiation wavelength risk exposure index", example = "3.1")
        double avgUvIndex,

        @Schema(name = "avg_solar_radiation_wm2", description = "Mean total raw incoming electromagnetic light radiation power density over an area segment", example = "345.5")
        double avgSolarRadiationWm2,

        @Schema(name = "avg_lux", description = "Calculated total perceived illumination luminous flux density spread over surface areas", example = "12500.0")
        double avgLux,

        @Schema(name = "avg_visibility_m", description = "Calculated horizontal transparent view displacement clearance capability threshold before light scatter", example = "10000.0")
        double avgVisibilityM
) {
}