package me.neobliz1.ecomonitoring.platform.history.domain.model.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.UUID;


@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table(name = WeatherTelemetryStationTransaction.TABLE_NAME)
@Entity(name = WeatherTelemetryStationTransaction.TABLE_NAME)
public class WeatherTelemetryStationTransaction {

    public static final String TABLE_NAME = "weather_telemetry_station_transaction_ids";

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    @Column(name = "id", updatable = false, nullable = false)
    private UUID id;

    @Column(name = "tx_id_ingestion", length = 50)
    private String txIdIngestion;

    @Column(name = "tx_id_history", length = 50)
    private String txIdHistory;
}