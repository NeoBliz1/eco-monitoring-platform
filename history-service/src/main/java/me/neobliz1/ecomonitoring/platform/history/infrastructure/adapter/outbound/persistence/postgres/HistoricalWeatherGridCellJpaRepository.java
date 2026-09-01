package me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter.outbound.persistence.postgres;

import me.neobliz1.ecomonitoring.platform.history.domain.model.entity.WeatherGridCellMetric;
import me.neobliz1.ecomonitoring.platform.history.domain.model.entity.WeatherGridCellMetricId;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

@Repository
public interface HistoricalWeatherGridCellJpaRepository extends JpaRepository<WeatherGridCellMetric, WeatherGridCellMetricId> {

    @Query(value = """
            SELECT * FROM #{#entityName} m
            WHERE m.bucket_id = :bucketId
              AND CAST(split_part(m.geohash, '#', 1) AS DOUBLE PRECISION) >= :minLat
              AND CAST(split_part(m.geohash, '#', 1) AS DOUBLE PRECISION) <= :maxLat
              AND CAST(split_part(m.geohash, '#', 2) AS DOUBLE PRECISION) >= :minLon
              AND CAST(split_part(m.geohash, '#', 2) AS DOUBLE PRECISION) <= :maxLon
            """, nativeQuery = true)
    List<WeatherGridCellMetric> findByBucketIdAndSpatialBox(
            @Param("bucketId") UUID bucketId,
            @Param("minLat") double minLat,
            @Param("maxLat") double maxLat,
            @Param("minLon") double minLon,
            @Param("maxLon") double maxLon
    );
}