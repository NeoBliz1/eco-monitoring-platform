package me.neobliz1.ecomonitoring.platform.ingestion.infrastructure.adapter.outbound.grpc.vector;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import me.neobliz1.ecomonitoring.platform.ingestion.domain.port.inbound.TelemetryIngestionService;
import me.neobliz1.ecomonitoring.platform.model.exception.PipelineTimeoutException;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.WeatherPacket;
import reactor.core.publisher.Mono;
import vector.PushEventsRequest;
import vector.PushEventsResponse;
import vector.VectorGrpc;

@Slf4j
@RequiredArgsConstructor
public class TelemetryIngestionAdapter implements TelemetryIngestionService {

    private final VectorGrpc.VectorStub asyncStub;
    private final VectorGrpc.VectorBlockingStub blockingStub;
    private final TelemetryPayloadMapper telemetryPayloadMapper;

    @Override
    public Mono<Boolean> processTelemetryPacket(WeatherPacket packet) {
        PushEventsRequest request = telemetryPayloadMapper.toPushRequest(packet);
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
                        if(log.isDebugEnabled()) {
                            log.debug("Mono request sent successfully, station id: {}", packet.getStationId());
                        }
                    }
                })
        );
    }

    @Override
    @SuppressWarnings({ "ResultOfMethodCallIgnored" })
    public boolean processTelemetryPacketVirtual(WeatherPacket packet) {
        try {
            blockingStub.pushEvents(telemetryPayloadMapper.toPushRequest(packet));
            if(log.isDebugEnabled()) {
                log.debug("Virtual request sent successfully, station id: {}", packet.getStationId());
            }
            return true;
        } catch(StatusRuntimeException e) {
            if(Status.Code.DEADLINE_EXCEEDED.equals(e.getStatus().getCode())) {
                throw new PipelineTimeoutException();
            }
            throw e;
        }
    }
}
