package me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter.outbound.persistence.postgres.jpa;

import jakarta.persistence.QueryHint;
import me.neobliz1.ecomonitoring.platform.history.domain.model.entity.WeatherGridCellMetric;
import me.neobliz1.ecomonitoring.platform.history.domain.model.entity.WeatherGridCellMetricId;
import org.hibernate.jpa.HibernateHints;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.QueryHints;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.Collection;
import java.util.List;
import java.util.UUID;

@Repository
public interface HistoricalWeatherGridCellJpaRepository extends JpaRepository<WeatherGridCellMetric, WeatherGridCellMetricId> {

    @QueryHints(@QueryHint(name = HibernateHints.HINT_CACHEABLE, value = "true"))
    @Query("""
            SELECT m FROM WeatherGridCellMetric m
            WHERE m.id.bucketId = :bucketId
              AND CAST(function('split_part', m.id.geohash, '#', 1) as double) >= :minLat
              AND CAST(function('split_part', m.id.geohash, '#', 1) as double) <= :maxLat
              AND CAST(function('split_part', m.id.geohash, '#', 2) as double) >= :minLon
              AND CAST(function('split_part', m.id.geohash, '#', 2) as double) <= :maxLon
            """)
    List<WeatherGridCellMetric> findByBucketIdAndSpatialBox(
            @Param("bucketId") UUID bucketId,
            @Param("minLat") double minLat,
            @Param("maxLat") double maxLat,
            @Param("minLon") double minLon,
            @Param("maxLon") double maxLon
    );

    @QueryHints(@QueryHint(name = HibernateHints.HINT_CACHEABLE, value = "true"))
    @Query("SELECT m FROM WeatherGridCellMetric m WHERE m.id.bucketId = :bucketId AND m.id.geohash IN :geohashes")
    List<WeatherGridCellMetric> findSpecificCellsForMerge(
            @Param("bucketId") UUID bucketId,
            @Param("geohashes") Collection<String> geohashes
    );

    List<WeatherGridCellMetric> findAllByIdBucketId(UUID bucketId);
}