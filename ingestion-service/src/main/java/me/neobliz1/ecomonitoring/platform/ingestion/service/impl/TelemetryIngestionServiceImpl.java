package me.neobliz1.ecomonitoring.platform.ingestion.service.impl;

import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import me.neobliz1.ecomonitoring.platform.ingestion.service.TelemetryIngestionService;
import me.neobliz1.ecomonitoring.platform.model.exception.PipelineTimeoutException;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.WeatherPacket;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import reactor.core.publisher.Mono;
import vector.EventWrapper;
import vector.Log;
import vector.PushEventsRequest;
import vector.PushEventsResponse;
import vector.Value;
import vector.ValueMap;
import vector.VectorGrpc;

@RequiredArgsConstructor
public class TelemetryIngestionServiceImpl implements TelemetryIngestionService {

    private final VectorGrpc.VectorStub asyncStub;
    private final VectorGrpc.VectorBlockingStub blockingStub;

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
        PushEventsRequest request = buildVectorPushRequest(packet);
        try {
            blockingStub.pushEvents(request);
            return true;
        } catch(StatusRuntimeException e) {
            if(Status.Code.DEADLINE_EXCEEDED.equals(e.getStatus().getCode())) {
                throw new PipelineTimeoutException();
            }
            throw e;
        }
    }

    private PushEventsRequest buildVectorPushRequest(WeatherPacket packet) {
        // 1. Assign the payload to raw_bytes (field 1)
        Value byteValue = Value.newBuilder()
                .setRawBytes(ByteString.copyFrom(packet.toByteArray()))
                .build();

        // 2. Wrap it inside the log payload's map fields
        ValueMap fieldsMap = ValueMap.newBuilder()
                .putFields("raw_protobuf_packet", byteValue)
                .build();

        Value logMapValue = Value.newBuilder()
                .setMap(fieldsMap)
                .build();

        // 3. Attach the value map container directly to field 2 of the Log event
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
