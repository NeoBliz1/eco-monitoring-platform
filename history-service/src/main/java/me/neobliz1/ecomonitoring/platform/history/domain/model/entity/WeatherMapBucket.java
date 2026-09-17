package me.neobliz1.ecomonitoring.platform.history.domain.model.entity;

import static me.neobliz1.ecomonitoring.platform.history.domain.model.constant.HistoricalCacheConstants.BUCKETS_REGION;
import static me.neobliz1.ecomonitoring.platform.history.domain.model.constant.HistoricalCacheConstants.BUCKET_METRICS_REGION;

import jakarta.persistence.Cacheable;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.Id;
import jakarta.persistence.OneToMany;
import jakarta.persistence.PostLoad;
import jakarta.persistence.PostPersist;
import jakarta.persistence.Table;
import jakarta.persistence.Transient;
import jakarta.persistence.UniqueConstraint;
import jakarta.persistence.Version;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.FieldNameConstants;
import org.hibernate.annotations.Cache;
import org.hibernate.annotations.CacheConcurrencyStrategy;
import org.springframework.data.domain.Persistable;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.UUID;

@Getter
@Setter
@Cacheable
@NoArgsConstructor
@FieldNameConstants
@Entity(name = WeatherMapBucket.TABLE_NAME)
@Cache(region = BUCKETS_REGION, usage = CacheConcurrencyStrategy.READ_WRITE)
@Table(name = WeatherMapBucket.TABLE_NAME, uniqueConstraints = {
        @UniqueConstraint(columnNames = { "timestamp_bucket", "interval_minutes" })
})
public class WeatherMapBucket implements Persistable<UUID> {

    public static final String TABLE_NAME = "weather_map_buckets";

    @Id
    private UUID id;

    @Column(name = "timestamp_bucket", nullable = false)
    private long timestampBucket;

    @Column(name = "interval_minutes", nullable = false)
    private int intervalMinutes;

    @Version
    @Column(name = "version", nullable = false)
    private int version;

    @Cache(region = BUCKET_METRICS_REGION, usage = CacheConcurrencyStrategy.READ_WRITE)
    @OneToMany(mappedBy = "bucket", fetch = FetchType.LAZY, orphanRemoval = true)
    private Set<WeatherGridCellMetric> gridCells = new LinkedHashSet<>();

    @Transient
    private boolean isNewRecord = true;

    public WeatherMapBucket(UUID id, Long timestampBucket, Integer intervalMinutes) {
        this.id = id;
        this.timestampBucket = timestampBucket;
        this.intervalMinutes = intervalMinutes;
        this.version = 0;
    }

    @Override
    public boolean isNew() {
        return this.isNewRecord;
    }

    @PostLoad
    @PostPersist
    public void markNotNew() {
        this.isNewRecord = false;
    }
}
