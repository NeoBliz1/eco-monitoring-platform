package me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter.inbound.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import me.neobliz1.ecomonitoring.platform.history.domain.model.entity.WeatherTelemetryDlqRecord;
import me.neobliz1.ecomonitoring.platform.history.domain.port.outbound.HistoricalPersistenceRepository;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.WeatherMap;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;

import java.util.UUID;

@Slf4j
@RequiredArgsConstructor
public class HistoricalTelemetryDlqListener {

    private final HistoricalPersistenceRepository historicalPersistenceRepository;

    @KafkaListener(
            topics = "${spring.kafka.topic.weather-history}.DLT",
            groupId = "${spring.kafka.consumer.group-id}-dlq-group"
    )
    public void consumeDeadLetters(@NonNull ConsumerRecord<String, WeatherMap> record,
                                   @Header(name = KafkaHeaders.DLT_EXCEPTION_MESSAGE, required = false) String exceptionMessage,
                                   @Header(name = KafkaHeaders.DLT_EXCEPTION_STACKTRACE, required = false) String stacktrace) {
        logDlqRecordAsError(record, exceptionMessage);
        WeatherMap deadPayload = record.value();
        WeatherTelemetryDlqRecord dlqRecord = new WeatherTelemetryDlqRecord(UUID.randomUUID(),
                record.topic(),
                record.partition(),
                record.offset(),
                exceptionMessage,
                stacktrace,
                deadPayload==null?null:deadPayload.toByteArray()
        );
        tryToPersistDlqRecordToDb(dlqRecord);
    }

    private void logDlqRecordAsError(@NonNull ConsumerRecord<String, WeatherMap> record, @Nullable String exceptionMessage) {
        WeatherMap deadPayload = record.value();
        long timestampBucket = deadPayload==null?0L:deadPayload.getTimestampBucket();
        int gridCellsCount = deadPayload==null?0:deadPayload.getGridCellsCount();
        String dlqStatMsg = String.format("Timestamp Bucket: %s, Grid Cells Count: %d", timestampBucket, gridCellsCount);
        log.error("""
                        Message exiled to Dead Letter Topic!
                        Original Location -> Topic: {}, Partition: {}, Offset: {}
                        Crash Cause       -> {}
                        Payload Content   -> {}""",
                record.topic(),
                record.partition(),
                record.offset(),
                exceptionMessage,
                dlqStatMsg
        );
    }

    private void tryToPersistDlqRecordToDb(@NonNull WeatherTelemetryDlqRecord dltRecord) {
        try {
            historicalPersistenceRepository.persistDltRecord(dltRecord);
        } catch(Exception e) {
            log.error("Failed to archive or alert on DLQ record at partition {} offset {}", dltRecord.getPartitionId(),
                    dltRecord.getOriginalOffset(), e);
            throw e;
        }
    }
}
