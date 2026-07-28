package me.neobliz1.ecomonitoring.platform.ingestion.infrastructure.adapter.outbound.grpc.vector;

import static me.neobliz1.ecomonitoring.platform.common.constant.PlatformConstants.RAW_PROTOBUF_PACKET;
import static me.neobliz1.ecomonitoring.platform.common.constant.PlatformConstants.SCHEMA_REGISTRY_URL;

import com.google.protobuf.ByteString;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.WeatherPacket;
import org.springframework.beans.factory.annotation.Value;
import vector.EventWrapper;
import vector.Log;
import vector.PushEventsRequest;
import vector.ValueMap;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@RequiredArgsConstructor
public class TelemetryPayloadMapper {

    private SchemaRegistryClient schemaRegistryClient;

    @Value("${spring.kafka.streams.properties.schema.registry.url}")
    private String schemaRegistryUrl;
    @Value("${spring.kafka.topic.weather-live}")
    private String kafkaIngestionLiveTopic;

    @PostConstruct
    public void init() {
        this.schemaRegistryClient = new CachedSchemaRegistryClient(
                schemaRegistryUrl,
                100,
                List.of(new ProtobufSchemaProvider()),
                Collections.emptyMap()
        );
    }


    public PushEventsRequest toPushRequest(WeatherPacket packet) {
        byte[] verifiedConfluentBytes = serializeToConfluentProtobuf(packet);

        vector.Value byteValue = vector.Value.newBuilder()
                .setRawBytes(ByteString.copyFrom(verifiedConfluentBytes))
                .build();

        ValueMap fieldsMap = ValueMap.newBuilder()
                .putFields(RAW_PROTOBUF_PACKET, byteValue)
                .build();

        vector.Value logMapValue = vector.Value.newBuilder()
                .setMap(fieldsMap)
                .build();

        Log vectorLog = Log.newBuilder()
                .setValue(logMapValue)
                .build();

        EventWrapper eventWrapper = EventWrapper.newBuilder()
                .setLog(vectorLog)
                .build();

        return PushEventsRequest.newBuilder()
                .addEvents(eventWrapper)
                .build();
    }

    private byte[] serializeToConfluentProtobuf(WeatherPacket packet) {
        Map<String, Object> serializerConfig = new HashMap<>();
        serializerConfig.put(SCHEMA_REGISTRY_URL, schemaRegistryUrl);
        try(KafkaProtobufSerializer<WeatherPacket> serializer = new KafkaProtobufSerializer<>(schemaRegistryClient)) {
            serializer.configure(serializerConfig, false);
            return serializer.serialize(kafkaIngestionLiveTopic, packet);
        } catch(Exception e) {
            log.error("Failed to perform Confluent Protobuf serialization for packet ID: {}", packet.getStationId(), e);
            throw e;
        }
    }
}
