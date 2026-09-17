package me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter.outbound.persistence.postgres.jpa;

import me.neobliz1.ecomonitoring.platform.history.domain.model.entity.WeatherTelemetryDltRecord;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.UUID;

@Repository
public interface HistoricalWeatherTelemetryDltJpaRepository extends JpaRepository<WeatherTelemetryDltRecord, UUID> {
}