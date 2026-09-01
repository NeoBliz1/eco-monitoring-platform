package me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.history.grpc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import lombok.Getter;
import lombok.Setter;
import me.neobliz1.ecomonitoring.platform.model.exception.WeatherMapDataNotFoundException;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.GridCellLayers;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.WeatherMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.grpc.client.autoconfigure.GrpcClientAutoConfiguration;
import org.springframework.boot.grpc.server.autoconfigure.GrpcServerAutoConfiguration;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import weather.history.HistoryServiceGrpc;
import weather.history.SpatialBoxRequest;

class TelemetryQueryGrpcAdapterTest {

    public static final double MIN_LAT = 55.0;
    public static final double MAX_LAT = 56.0;
    public static final double MIN_LON = 60.0;
    public static final double MAX_LON = 61.0;
    public static final String GEOHASH = "55.5#60.5";
    private static final long ACTIVE_BUCKET_FLOOR = 1718845200000L;
    private static ApplicationContextRunner contextRunner;

    @BeforeAll
    static void setupSuite() {
        contextRunner = new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(
                        GrpcClientAutoConfiguration.class,
                        GrpcServerAutoConfiguration.class
                ))
                .withPropertyValues(
                        "spring.kafka.streams.pipeline.name.aggregation-processor.interval=100000",
                        "spring.main.web-application-type=none",
                        "spring.cloud.consul.discovery.enabled=false",
                        "spring.cloud.consul.config.enabled=false",
                        "spring.grpc.server.port=9099"
                )
                .withBean(SslBundles.class, () -> Mockito.mock(SslBundles.class))
                .withUserConfiguration(MockHistoryServerConfig.class)
                .withBean(TelemetryQueryGrpcAdapter.class);
    }

    @AfterEach
    void cleanMockState() {
        contextRunner.run(context -> context.getBean(MockHistoryService.class).reset());
    }

    @Test
    void shouldReturnWeatherMap_whenHistoryServiceRespondsSuccessfully() {
        contextRunner.run(context -> {
            MockHistoryService mockHistoryService = context.getBean(MockHistoryService.class);
            TelemetryQueryGrpcAdapter adapter = context.getBean(TelemetryQueryGrpcAdapter.class);
            WeatherMap expectedResponse = WeatherMap.newBuilder()
                    .setTimestampBucket(ACTIVE_BUCKET_FLOOR)
                    .setIntervalMinutes(15)
                    .putGridCells(GEOHASH, GridCellLayers.newBuilder()
                            .setGeohash(GEOHASH)
                            .setReadingCount(5)
                            .setAvgTemperature(22.5)
                            .build())
                    .build();
            mockHistoryService.setNextResponse(expectedResponse);

            WeatherMap actualResponse = adapter.findFilteredGridDataBySpatialBoxInArchive(
                    ACTIVE_BUCKET_FLOOR, MIN_LAT, MAX_LAT, MIN_LON, MAX_LON
            );

            assertThat(actualResponse).isNotNull();
            assertThat(actualResponse.getTimestampBucket()).isEqualTo(ACTIVE_BUCKET_FLOOR);
            assertThat(actualResponse.getIntervalMinutes()).isEqualTo(15);
            assertThat(actualResponse.getGridCellsOrThrow(GEOHASH).getAvgTemperature()).isEqualTo(22.5);
            SpatialBoxRequest capturedRequest = mockHistoryService.getLastCapturedRequest();
            assertThat(capturedRequest.getTimestampBucket()).isEqualTo(ACTIVE_BUCKET_FLOOR);
            assertThat(capturedRequest.getMinLat()).isEqualTo(MIN_LAT);
        });
    }

    @Test
    void shouldThrowWeatherMapDataNotFoundException_whenHistoryServiceFailsOrReturnsRpcError() {
        contextRunner.run(context -> {
            MockHistoryService mockHistoryService = context.getBean(MockHistoryService.class);
            TelemetryQueryGrpcAdapter adapter = context.getBean(TelemetryQueryGrpcAdapter.class);
            mockHistoryService.setNextException(
                    Status.NOT_FOUND.withDescription("No archival bucket available").asRuntimeException()
            );

            assertThatThrownBy(() -> adapter.findFilteredGridDataBySpatialBoxInArchive(
                    ACTIVE_BUCKET_FLOOR, MIN_LAT, MAX_LAT, MIN_LON, MAX_LON
            ))
                    .isInstanceOf(WeatherMapDataNotFoundException.class);
        });
    }

    static class MockHistoryServerConfig {

        @Bean
        public MockHistoryService mockHistoryService() {
            return new MockHistoryService();
        }

        @Bean
        public HistoryServiceGrpc.HistoryServiceBlockingStub historyServiceBlockingStub() {
            ManagedChannel testNetworkChannel = ManagedChannelBuilder
                    .forAddress("localhost", 9099)
                    .directExecutor()
                    .usePlaintext()
                    .build();
            return HistoryServiceGrpc.newBlockingStub(testNetworkChannel);
        }
    }

    public static class MockHistoryService extends HistoryServiceGrpc.HistoryServiceImplBase {

        @Getter
        private SpatialBoxRequest lastCapturedRequest;

        @Setter
        private WeatherMap nextResponse;

        @Setter
        private Exception nextException;

        public void reset() {
            this.lastCapturedRequest = null;
            this.nextResponse = null;
            this.nextException = null;
        }

        @Override
        public void findFilteredGridDataBySpatialBox(SpatialBoxRequest request, StreamObserver<WeatherMap> responseObserver) {
            this.lastCapturedRequest = request;
            if(nextException!=null) {
                responseObserver.onError(nextException);
            } else if(nextResponse!=null) {
                responseObserver.onNext(nextResponse);
                responseObserver.onCompleted();
            } else {
                responseObserver.onError(Status.INTERNAL.withDescription("No mock criteria configured").asRuntimeException());
            }
        }
    }
}