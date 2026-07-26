package me.neobliz1.ecomonitoring.platform.ingestion.infrastructure.delivery.grpc;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import me.neobliz1.ecomonitoring.platform.ingestion.domain.service.TelemetryIngestionService;
import me.neobliz1.ecomonitoring.platform.model.exception.PipelineTimeoutException;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.WeatherPacket;
import reactor.core.publisher.Mono;
import vector.PushEventsRequest;
import vector.PushEventsResponse;
import vector.VectorGrpc;

@Slf4j
@RequiredArgsConstructor
public class VectorGrpcTelemetryIngestionAdapter implements TelemetryIngestionService {

    private final VectorGrpc.VectorStub asyncStub;
    private final VectorGrpc.VectorBlockingStub blockingStub;
    private final VectorPayloadMapper vectorPayloadMapper;

    @Override
    public Mono<Boolean> processTelemetryPacket(WeatherPacket packet) {
        PushEventsRequest request = vectorPayloadMapper.toPushRequest(packet);

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
            blockingStub.pushEvents(vectorPayloadMapper.toPushRequest(packet));
            return true;
        } catch(StatusRuntimeException e) {
            if(Status.Code.DEADLINE_EXCEEDED.equals(e.getStatus().getCode())) {
                throw new PipelineTimeoutException();
            }
            throw e;
        }
    }
}
