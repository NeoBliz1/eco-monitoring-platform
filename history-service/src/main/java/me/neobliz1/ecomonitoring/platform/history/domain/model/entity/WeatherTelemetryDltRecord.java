package me.neobliz1.ecomonitoring.platform.history.domain.model.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.hibernate.annotations.CreationTimestamp;

import java.time.OffsetDateTime;
import java.util.UUID;

@Getter
@Setter
@NoArgsConstructor
@Entity
@Table(name = WeatherTelemetryDltRecord.TABLE_NAME)
public class WeatherTelemetryDltRecord {

    public static final String TABLE_NAME = "weather_telemetry_dlt_records";

    @Id
    @Column(name = "id", nullable = false)
    private UUID id;

    @Column(name = "original_topic", nullable = false)
    private String originalTopic;

    @Column(name = "partition_id", nullable = false)
    private int partitionId;

    @Column(name = "original_offset", nullable = false)
    private long originalOffset;

    @Column(name = "exception_message", columnDefinition = "TEXT")
    private String exceptionMessage;

    @Column(name = "exception_stacktrace", columnDefinition = "TEXT")
    private String exceptionStacktrace;

    @Column(name = "raw_payload_bytes")
    private byte[] rawPayloadBytes;

    @CreationTimestamp
    @Column(name = "exiled_at", nullable = false, updatable = false)
    private OffsetDateTime exiledAt;

    public WeatherTelemetryDltRecord(UUID id, String originalTopic, int partitionId, long originalOffset,
                                     String exceptionMessage, String exceptionStacktrace, byte[] rawPayloadBytes) {
        this.id = id;
        this.originalTopic = originalTopic;
        this.partitionId = partitionId;
        this.originalOffset = originalOffset;
        this.exceptionMessage = exceptionMessage;
        this.exceptionStacktrace = exceptionStacktrace;
        this.rawPayloadBytes = rawPayloadBytes;
    }
}