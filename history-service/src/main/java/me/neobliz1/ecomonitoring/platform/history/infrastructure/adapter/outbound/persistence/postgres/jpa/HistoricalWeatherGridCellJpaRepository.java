package me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter.outbound.persistence.postgres.jpa;

import static me.neobliz1.ecomonitoring.platform.history.domain.model.constant.HistoricalCacheConstants.BUCKET_GRID_CELL_LAYERS_L2_REGION;

import jakarta.persistence.QueryHint;
import me.neobliz1.ecomonitoring.platform.history.domain.model.entity.WeatherGridCellLayer;
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
public interface HistoricalWeatherGridCellJpaRepository extends JpaRepository<WeatherGridCellLayer, WeatherGridCellMetricId> {

    @QueryHints(value = {
            @QueryHint(name = HibernateHints.HINT_CACHEABLE, value = "true"),
            @QueryHint(name = HibernateHints.HINT_CACHE_REGION, value = BUCKET_GRID_CELL_LAYERS_L2_REGION)
    }, forCounting = false)
    @Query("""
            SELECT m FROM WeatherGridCellLayer m
            WHERE m.id.bucketId = :bucketId
              AND m.latitude >= :minLat
              AND m.latitude <= :maxLat
              AND m.longitude >= :minLon
              AND m.longitude <= :maxLon
            """)
    List<WeatherGridCellLayer> findByBucketIdAndSpatialBox(
            @Param("bucketId") UUID bucketId,
            @Param("minLat") double minLat,
            @Param("maxLat") double maxLat,
            @Param("minLon") double minLon,
            @Param("maxLon") double maxLon
    );

    @QueryHints(value = {
            @QueryHint(name = HibernateHints.HINT_CACHEABLE, value = "true"),
            @QueryHint(name = HibernateHints.HINT_CACHE_REGION, value = BUCKET_GRID_CELL_LAYERS_L2_REGION)
    }, forCounting = false)
    @Query("SELECT m FROM WeatherGridCellLayer m WHERE m.id.bucketId = :bucketId AND m.id.geohash IN :geohashes")
    List<WeatherGridCellLayer> findSpecificGridCellLayersForMerge(
            @Param("bucketId") UUID bucketId,
            @Param("geohashes") Collection<String> geohashes
    );

    List<WeatherGridCellLayer> findAllByIdBucketId(UUID bucketId);
}