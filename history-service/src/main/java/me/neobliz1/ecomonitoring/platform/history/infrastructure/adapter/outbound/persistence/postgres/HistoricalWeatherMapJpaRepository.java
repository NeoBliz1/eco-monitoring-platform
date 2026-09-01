package me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter.outbound.persistence.postgres;

import me.neobliz1.ecomonitoring.platform.history.domain.model.entity.WeatherGridCellMetric;
import me.neobliz1.ecomonitoring.platform.history.domain.model.entity.WeatherMapBucket;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.Optional;
import java.util.UUID;

@Repository
public interface HistoricalWeatherMapJpaRepository extends JpaRepository<WeatherMapBucket, UUID> {

    String UPSERT_QUERY = """
            WITH upserted_bucket AS (
                INSERT INTO #{#entityName} (id, timestamp_bucket, interval_minutes, version)
                VALUES (:id, :timestamp, :interval, 0)
                ON CONFLICT (timestamp_bucket, interval_minutes)
                DO UPDATE SET version = #{#entityName}.version
                RETURNING *
            )
            SELECT b.*, m.*
            FROM upserted_bucket b
            LEFT JOIN\s"""
            +WeatherGridCellMetric.TABLE_NAME
            +" m ON b.id = m.bucket_id";

    @Query(value = UPSERT_QUERY, nativeQuery = true)
    WeatherMapBucket upsertBucket(@Param("id") UUID id, @Param("timestamp") long timestamp, @Param("interval") int interval);

    Optional<WeatherMapBucket> findByTimestampBucketAndIntervalMinutes(Long timestampBucket, Integer intervalMinutes);
}