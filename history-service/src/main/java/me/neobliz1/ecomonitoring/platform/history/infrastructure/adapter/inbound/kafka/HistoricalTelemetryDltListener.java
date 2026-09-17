package me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter.inbound.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import me.neobliz1.ecomonitoring.platform.history.domain.model.entity.WeatherTelemetryDltRecord;
import me.neobliz1.ecomonitoring.platform.history.domain.port.outbound.HistoricalPersistenceRepository;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.WeatherMap;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.jspecify.annotations.NonNull;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;
import java.util.UUID;

@Slf4j
@Component
@RequiredArgsConstructor
public class HistoricalTelemetryDltListener {

    private final HistoricalPersistenceRepository historicalPersistenceRepository;

    private static void logDlqError(@NonNull ConsumerRecord<String, WeatherMap> record, String originalTopic, String exceptionMessage, long originalOffset, WeatherMap deadPayload) {
        log.error("""
                        Message exiled to Dead Letter Topic!
                        Original Location -> Topic: {}, Partition: {}, Offset: {}
                        Crash Cause       -> {}
                        Payload Content   -> {}""",
                originalTopic,
                record.partition(),
                originalOffset,
                exceptionMessage,
                deadPayload!=null
                        ?String.format("Timestamp Bucket: %s, Grid Cells Count: %d", deadPayload.getTimestampBucket(), deadPayload.getGridCellsCount())
                        :"[NULL/CORRUPT PAYLOAD] - Likely a deserialization poison pill."
        );
    }

    @KafkaListener(
            topics = "${spring.kafka.topic.weather-history}.DLT",
            groupId = "${spring.kafka.consumer.group-id}-dlq-group"
    )
    public void consumeDeadLetters(
            @NonNull ConsumerRecord<String, WeatherMap> record,
            @Header(name = KafkaHeaders.DLT_ORIGINAL_TOPIC, required = false) String originalTopic,
            @Header(name = KafkaHeaders.DLT_ORIGINAL_OFFSET, required = false) byte[] originalOffsetBytes,
            @Header(name = KafkaHeaders.DLT_EXCEPTION_MESSAGE, required = false) String exceptionMessage,
            @Header(name = KafkaHeaders.DLT_EXCEPTION_STACKTRACE, required = false) String stacktrace) {

        long originalOffset = -1;
        if(originalOffsetBytes!=null && originalOffsetBytes.length>=8) {
            originalOffset = ByteBuffer.wrap(originalOffsetBytes).getLong();
        }
        WeatherMap deadPayload = record.value();
        logDlqError(record, originalTopic, exceptionMessage, originalOffset, deadPayload);
        if(deadPayload!=null) {
            log.error("Payload Content {Timestamp Bucket: {}, Grid Cells Count: {}}",
                    deadPayload.getTimestampBucket(), deadPayload.getGridCellsCount());
        } else {
            log.error("Payload Content {[NULL/CORRUPT PAYLOAD] - Likely a deserialization poison pill.}");
        }
        try {
            byte[] rawBytes = deadPayload!=null?deadPayload.toByteArray():null;
            WeatherTelemetryDltRecord dltRecord = new WeatherTelemetryDltRecord(
                    UUID.randomUUID(),
                    originalTopic!=null?originalTopic:record.topic(),
                    record.partition(),
                    originalOffset,
                    exceptionMessage,
                    stacktrace,
                    rawBytes
            );
            historicalPersistenceRepository.persistDltRecord(dltRecord);
        } catch(Exception e) {
            log.error("Failed to archive or alert on DLQ record at partition {} offset {}", record.partition(), record.offset(), e);
            throw e;
        }
    }
}
