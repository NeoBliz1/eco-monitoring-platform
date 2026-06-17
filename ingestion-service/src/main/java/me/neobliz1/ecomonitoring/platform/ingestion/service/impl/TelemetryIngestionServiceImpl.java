package me.neobliz1.ecomonitoring.platform.ingestion.service.impl;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import me.neobliz1.ecomonitoring.platform.ingestion.service.TelemetryIngestionService;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.VectorIngestServiceGrpc;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.VectorPayload;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.VectorResponse;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.WeatherPacket;
import reactor.core.publisher.Mono;

public class TelemetryIngestionServiceImpl implements TelemetryIngestionService {

    private final VectorIngestServiceGrpc.VectorIngestServiceStub asyncStub;

    public TelemetryIngestionServiceImpl(ManagedChannel channel) {
        this.asyncStub = VectorIngestServiceGrpc.newStub(channel);
    }

    @Override
    public Mono<Boolean> processTelemetryPacket(WeatherPacket packet) {
        VectorPayload vectorPayload = VectorPayload.newBuilder()
                .setRawProtobufPacket(ByteString.copyFrom(packet.toByteArray()))
                .build();
        return Mono.create(sink ->
                asyncStub.streamTelemetry(vectorPayload,
                        new StreamObserver<>() {
                            @Override
                            public void onNext(VectorResponse vectorResponse) {
                                sink.success(vectorResponse.getAccepted());
                            }

                            @Override
                            public void onError(Throwable throwable) {
                                sink.error(throwable);
                            }

                            @Override
                            public void onCompleted() {
                            }
                        }
                )
        );
    }
}
