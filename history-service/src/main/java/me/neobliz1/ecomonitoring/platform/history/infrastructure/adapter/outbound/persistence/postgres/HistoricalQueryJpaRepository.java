package me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter.outbound.persistence.postgres;

import me.neobliz1.ecomonitoring.platform.history.domain.model.entity.WeatherMapBucket;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;
import java.util.UUID;

@Repository
public interface HistoricalQueryJpaRepository extends JpaRepository<WeatherMapBucket, UUID> {

    Optional<WeatherMapBucket> findByTimestampBucketAndIntervalMinutes(Long timestampBucket, Integer intervalMinutes);
}
