package me.neobliz1.ecomonitoring.platform.ingestion.service.impl;

import static me.neobliz1.ecomonitoring.platform.common.constant.PlatformConstants.SCHEMA_REGISTRY_URL;

import com.google.protobuf.ByteString;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import me.neobliz1.ecomonitoring.platform.common.constant.PlatformConstants;
import me.neobliz1.ecomonitoring.platform.ingestion.service.TelemetryIngestionService;
import me.neobliz1.ecomonitoring.platform.model.exception.PipelineTimeoutException;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.WeatherPacket;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import reactor.core.publisher.Mono;
import vector.EventWrapper;
import vector.Log;
import vector.PushEventsRequest;
import vector.PushEventsResponse;
import vector.ValueMap;
import vector.VectorGrpc;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@RequiredArgsConstructor
public class TelemetryIngestionServiceImpl implements TelemetryIngestionService {

    private final VectorGrpc.VectorStub asyncStub;
    private final VectorGrpc.VectorBlockingStub blockingStub;
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

    @Override
    public Mono<Boolean> processTelemetryPacket(WeatherPacket packet) {
        PushEventsRequest request = buildVectorPushRequest(packet);

        return Mono.create(sink ->
                asyncStub.pushEvents(request, new StreamObserver<>() {
                    @Override
                    public void onNext(PushEventsResponse response) {
                        sink.success(true);
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        sink.error(throwable);
                    }

                    @Override
                    public void onCompleted() {
                    }
                })
        );
    }

    @Override
    @SuppressWarnings({ "ResultOfMethodCallIgnored" })
    public boolean processTelemetryPacketVirtual(WeatherPacket packet) {
        try {
            blockingStub.pushEvents(buildVectorPushRequest(packet));
            return true;
        } catch(StatusRuntimeException e) {
            if(Status.Code.DEADLINE_EXCEEDED.equals(e.getStatus().getCode())) {
                throw new PipelineTimeoutException();
            }
            throw e;
        }
    }

    private PushEventsRequest buildVectorPushRequest(WeatherPacket packet) {
        byte[] verifiedConfluentBytes;
        Map<String, Object> serializerConfig = new HashMap<>();
        serializerConfig.put(SCHEMA_REGISTRY_URL, schemaRegistryUrl);
        try (KafkaProtobufSerializer<WeatherPacket> serializer = new KafkaProtobufSerializer<>(schemaRegistryClient)) {
            serializer.configure(serializerConfig, false);
            verifiedConfluentBytes = serializer.serialize(kafkaIngestionLiveTopic, packet);
        }
        vector.Value byteValue = vector.Value.newBuilder()
                .setRawBytes(ByteString.copyFrom(verifiedConfluentBytes))
                .build();
        ValueMap fieldsMap = ValueMap.newBuilder()
                .putFields(PlatformConstants.RAW_PROTOBUF_PACKET, byteValue)
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

    public static ResponseEntity<Void> getResponseEntity(Boolean isAccepted) {
        if(Boolean.TRUE.equals(isAccepted)) {
            return ResponseEntity.status(HttpStatus.ACCEPTED).build();
        } else {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).build();
        }
    }
}
