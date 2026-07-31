package me.neobliz1.ecomonitoring.platform.history.domain.model.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.UUID;

@Data
@Embeddable
@NoArgsConstructor
@AllArgsConstructor
public class WeatherGridCellMetricId implements Serializable {

    @Column(name = "bucket_id")
    private UUID bucketId;

    @Column(name = "geohash")
    private String geohash;
}
