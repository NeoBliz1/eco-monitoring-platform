package me.neobliz1.ecomonitoring.platform.history.domain.model.entity;

import static me.neobliz1.ecomonitoring.platform.history.domain.model.constant.HistoricalCacheConstants.METRICS_REGION;

import jakarta.persistence.Column;
import jakarta.persistence.EmbeddedId;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.MapsId;
import jakarta.persistence.PostLoad;
import jakarta.persistence.PostPersist;
import jakarta.persistence.PostUpdate;
import jakarta.persistence.Table;
import jakarta.persistence.Transient;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.hibernate.annotations.Cache;
import org.hibernate.annotations.CacheConcurrencyStrategy;
import org.jspecify.annotations.NonNull;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.data.domain.Persistable;

import java.util.UUID;

@Getter
@Setter
@Entity
@Cacheable
@NoArgsConstructor
@Table(name = WeatherGridCellMetric.TABLE_NAME)
@Cache(region = METRICS_REGION, usage = CacheConcurrencyStrategy.READ_WRITE)
public class WeatherGridCellMetric implements Persistable<WeatherGridCellMetricId> {

    public static final String TABLE_NAME = "weather_grid_cell_metrics";

    @EmbeddedId
    @Getter
    @Setter(AccessLevel.NONE)
    private WeatherGridCellMetricId id;

    @Transient
    private boolean isNewRecord = true;

    @MapsId("bucketId")
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "bucket_id", nullable = false)
    private WeatherMapBucket bucket;

    @Column(name = "reading_count", nullable = false)
    private int readingCount;

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

    public WeatherGridCellMetric(@NonNull WeatherMapBucket bucket, @NonNull String geohash) {
        this.bucket = bucket;
        this.id = new WeatherGridCellMetricId(bucket.getId(), geohash);
        setGeohash(geohash);
    }

    public String getGeohash() {
        return id.getGeohash();
    }

    public void setGeohash(@NonNull String geohash) {
        id.setGeohash(geohash);
    }
    public UUID getBucketId() {
        return id.getBucketId();
    }
    public void setBucketId(UUID uuid) {
        id.setBucketId(uuid);
    }

    @Override
    public boolean isNew() {
        return this.isNewRecord;
    }

    @PostLoad
    @PostUpdate
    @PostPersist
    public void markNotNew() {
        this.isNewRecord = false;
    }
}

