package me.neobliz1.ecomonitoring.platform.history.domain.model.entity;

import jakarta.persistence.CascadeType;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.OneToMany;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Entity
@Table(name = "weather_map_buckets")
@Getter
@Setter
@NoArgsConstructor
public class WeatherMapBucket {

    @Id
    private UUID id;

    @Column(name = "timestamp_bucket", nullable = false)
    private Long timestampBucket;

    @Column(name = "interval_minutes", nullable = false)
    private Integer intervalMinutes;

    // 🌟 THE OPTIMISTIC LOCK GATEKEEPER
    @Version
    @Column(name = "version", nullable = false)
    private Long version;

    @OneToMany(mappedBy = "bucket", cascade = CascadeType.ALL, orphanRemoval = true)
    private List<WeatherGridCellMetric> gridCells = new ArrayList<>();

    public WeatherMapBucket(UUID id, Long timestampBucket, Integer intervalMinutes) {
        this.id = id;
        this.timestampBucket = timestampBucket;
        this.intervalMinutes = intervalMinutes;
    }

    public void addCellMetric(WeatherGridCellMetric cell) {
        gridCells.add(cell);
        cell.setBucket(this);
    }
}
