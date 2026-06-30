package me.neobliz1.ecomonitoring.platform.ingestion.service.impl;

import static me.neobliz1.ecomonitoring.platform.model.exception.ErrorCode.PIPELINE_TIMEOUT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import me.neobliz1.ecomonitoring.platform.model.exception.PipelineTimeoutException;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.WeatherPacket;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import vector.PushEventsRequest;
import vector.PushEventsResponse;
import vector.VectorGrpc;

@ExtendWith(MockitoExtension.class)
public class TelemetryIngestionServiceImplUnitTest {

    @Mock
    private VectorGrpc.VectorStub asyncStub;
    @Mock
    private VectorGrpc.VectorBlockingStub blockingStub;
    @InjectMocks
    private TelemetryIngestionServiceImpl service;

    @Test
    void shouldReturnMonoTrue_whenStreamTelemetrySucceedsAndResponseIsAccepted() {
        WeatherPacket packet = WeatherPacket.newBuilder().build();
        PushEventsResponse fakeResponse = PushEventsResponse.newBuilder().build();
        doAnswer(invocation -> {
            io.grpc.stub.StreamObserver<PushEventsResponse> observer = invocation.getArgument(1);
            observer.onNext(fakeResponse);
            observer.onCompleted();
            return null;
        }).when(asyncStub).pushEvents(any(PushEventsRequest.class), any());

        Mono<Boolean> result = service.processTelemetryPacket(packet);

        StepVerifier.create(result)
                .expectNext(true)
                .verifyComplete();
    }

    @Test
    void shouldReturnTrue_whenProcessTelemetryPacketVirtualSucceeds() {
        WeatherPacket packet = WeatherPacket.newBuilder().build();
        PushEventsResponse fakeResponse = PushEventsResponse.newBuilder().build();
        when(blockingStub.pushEvents(any(PushEventsRequest.class))).thenReturn(fakeResponse);

        boolean result = service.processTelemetryPacketVirtual(packet);

        assertTrue(result);
    }

    @Test
    void shouldReturnMonoError_whenStreamTelemetryFailsWithGrpcError() {
        WeatherPacket packet = WeatherPacket.newBuilder().build();
        doAnswer(invocation -> {
            StreamObserver<PushEventsResponse> observer = invocation.getArgument(1);
            observer.onError(new StatusRuntimeException(Status.INTERNAL.withDescription("Internal Server Error")));
            return null;
        }).when(asyncStub).pushEvents(any(PushEventsRequest.class), any());

        Mono<Boolean> result = service.processTelemetryPacket(packet);

        StepVerifier.create(result)
                .expectErrorMatches(throwable -> {
                    if(!(throwable instanceof StatusRuntimeException grpcEx)) {
                        return false;
                    }
                    return grpcEx.getStatus().getCode()==Status.Code.INTERNAL
                            && "Internal Server Error".equals(grpcEx.getStatus().getDescription());
                })
                .verify();
    }

    @Test
    void shouldThrowPipelineTimeoutException_whenProcessTelemetryPacketVirtualFailsWithTimeout() {
        WeatherPacket packet = WeatherPacket.newBuilder().build();
        when(blockingStub.pushEvents(any(PushEventsRequest.class))).thenThrow(
                new StatusRuntimeException(Status.DEADLINE_EXCEEDED.withDescription("Deadline Exceeded"))
        );

        PipelineTimeoutException exception = assertThrows(PipelineTimeoutException.class, () -> service.processTelemetryPacketVirtual(packet));

        assertEquals(PIPELINE_TIMEOUT.getCodeStr(), exception.getErrorCode().getCodeStr());
    }
}
