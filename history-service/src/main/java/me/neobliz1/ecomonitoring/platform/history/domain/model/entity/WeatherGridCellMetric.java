package me.neobliz1.ecomonitoring.platform.history.domain.model.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.UUID;

@Entity
@Table(name = "weather_grid_cell_metrics")
@Getter
@Setter
@NoArgsConstructor
public class WeatherGridCellMetric {

    @Id
    private UUID id;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "bucket_id", nullable = false)
    private WeatherMapBucket bucket;

    @Column(nullable = false, length = 12)
    private String geohash;

    @Column(name = "reading_count", nullable = false)
    private Integer readingCount;

    // Sparse Environmental Attributes (Using Boxed Double to safely store NULL values)
    @Column(name = "avg_temperature")
    private Double avgTemperature;
    @Column(name = "avg_humidity")
    private Double avgHumidity;
    @Column(name = "avg_pressure")
    private Double avgPressure;
    @Column(name = "avg_leaf_wetness_pct")
    private Double avgLeaf_wetnessPct;

    @Column(name = "avg_wind_speed")
    private Double avgWindSpeed;
    @Column(name = "avg_wind_direction")
    private Double avgWindDirection;

    @Column(name = "avg_pm25")
    private Double avgPm25;
    @Column(name = "avg_pm10")
    private Double avgPm10;
    @Column(name = "avg_pm100")
    private Double avgPm100;

    @Column(name = "avg_voc")
    private Double avgVoc;
    @Column(name = "avg_noise_db")
    private Double avgNoiseDb;

    @Column(name = "avg_rain_mm")
    private Double avgRainMm;
    @Column(name = "avg_snow_cm")
    private Double avgSnowCm;
    @Column(name = "avg_evap_rate")
    private Double avgEvapRate;

    @Column(name = "avg_uv_index")
    private Double avgUvIndex;
    @Column(name = "avg_solar_radiation_wm2")
    private Double avgSolarRadiationWm2;
    @Column(name = "avg_lux")
    private Double avgLux;
    @Column(name = "avg_visibility_m")
    private Double avgVisibilityM;
}

