package me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter.inbound.grpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.github.neobliz1.validproto.config.HttpValidateProtoAutoConfiguration;
import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.testing.GrpcCleanupRule;
import me.neobliz1.ecomonitoring.platform.common.advice.GlobalPlatformGrpcAdviceEngine;
import me.neobliz1.ecomonitoring.platform.history.domain.model.entity.WeatherGridCellMetric;
import me.neobliz1.ecomonitoring.platform.history.domain.model.entity.WeatherMapBucket;
import me.neobliz1.ecomonitoring.platform.history.domain.port.inbound.HistoricalDataConvertService;
import me.neobliz1.ecomonitoring.platform.history.domain.port.outbound.HistoricalQueryRepository;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.GridCellLayers;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.WeatherMap;
import org.junit.Rule;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.aop.framework.AopProxyUtils;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.grpc.server.advice.GrpcAdviceDiscoverer;
import org.springframework.grpc.server.advice.GrpcAdviceExceptionHandler;
import org.springframework.grpc.server.advice.GrpcExceptionHandlerMethodResolver;
import weather.history.HistoryServiceGrpc;
import weather.history.SpatialBoxRequest;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;


public class HistoricalExternalCommunicationObserverTest {

    private static ApplicationContextRunner contextRunner;
    @Rule
    public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();
    private HistoricalQueryRepository historicalQueryRepository;
    private HistoricalDataConvertService weatherMapConverter;
    private HistoryServiceGrpc.HistoryServiceBlockingStub blockingStub;
    private io.grpc.Server server;

    @BeforeAll
    static void setupSuite() {
        contextRunner = new ApplicationContextRunner()
                .withPropertyValues("spring.main.web-application-type=none")
                .withUserConfiguration(HttpValidateProtoAutoConfiguration.class, TestConfig.class);
    }

    private static Stream<InvalidRequestTestCase> provideInvalidRequests() {
        return Stream.of(
                new InvalidRequestTestCase(
                        SpatialBoxRequest.newBuilder().setTimestampBucket(0).setTimeIntervalInMinutes(15),
                        "timestamp_bucket: must be greater than 0"
                ),
                new InvalidRequestTestCase(
                        SpatialBoxRequest.newBuilder().setTimestampBucket(100).setTimeIntervalInMinutes(0),
                        "time_interval_in_minutes: must be greater than 0" // 👈 FIX THIS FIELD STRING
                ),
                new InvalidRequestTestCase(
                        SpatialBoxRequest.newBuilder().setTimestampBucket(100).setTimeIntervalInMinutes(15).setMinLat(-90.1),
                        "min_lat: must be greater than or equal to -90"
                ),
                new InvalidRequestTestCase(
                        SpatialBoxRequest.newBuilder().setTimestampBucket(100).setTimeIntervalInMinutes(15).setMaxLat(90.1),
                        "max_lat: must be greater than or equal to -90"
                ),
                new InvalidRequestTestCase(
                        SpatialBoxRequest.newBuilder().setTimestampBucket(100).setTimeIntervalInMinutes(15).setMinLon(-180.1),
                        "min_lon: must be greater than or equal to -180"
                ),
                new InvalidRequestTestCase(
                        SpatialBoxRequest.newBuilder().setTimestampBucket(100).setTimeIntervalInMinutes(15).setMaxLon(180.1),
                        "max_lon: must be greater than or equal to -180"
                )
        );
    }

    @BeforeEach
    void setUp() {
        contextRunner.run(context -> {
            historicalQueryRepository = context.getBean(HistoricalQueryRepository.class);
            weatherMapConverter = context.getBean(HistoricalDataConvertService.class);
            HistoricalExternalCommunicationObserver serviceImpl = context.getBean(HistoricalExternalCommunicationObserver.class);
            GrpcAdviceExceptionHandler adviceHandler = configureAdviceHandler(context);
            ServerInterceptor adviceInterceptor = createAdviceInterceptor(adviceHandler);
            mockDefaultDatabaseBehavior();
            String serverName = InProcessServerBuilder.generateName();
            server = grpcCleanup.register(InProcessServerBuilder.forName(serverName)
                    .directExecutor()
                    .addService(serviceImpl)
                    .intercept(adviceInterceptor)
                    .build()
                    .start());
            blockingStub = HistoryServiceGrpc.newBlockingStub(
                    grpcCleanup.register(InProcessChannelBuilder.forName(serverName).directExecutor().build())
            );
        });
    }

    @AfterEach
    void tearDown() {
        if(server!=null) {
            server.shutdownNow();
        }
    }

    @Test
    void shouldReturnWeatherMapLayers_whenPayloadAndSpatialBoxAreValid() {
        SpatialBoxRequest validRequest = createValidBase().build();
        UUID targetBucketId = UUID.randomUUID();
        WeatherMapBucket mockBucket = mock(WeatherMapBucket.class);
        when(mockBucket.getId()).thenReturn(targetBucketId);
        when(historicalQueryRepository.findByTimestampBucketAndIntervalMinutes(1710000000L, 15))
                .thenReturn(Optional.of(mockBucket));
        WeatherGridCellMetric metric = mock(WeatherGridCellMetric.class);
        when(metric.getGeohash()).thenReturn("v123");
        when(historicalQueryRepository.findByBucketIdAndSpatialBox(targetBucketId, 45.0, 46.0, 12.0, 13.0))
                .thenReturn(List.of(metric));
        GridCellLayers layers = GridCellLayers.newBuilder().setReadingCount(5).build();
        when(weatherMapConverter.convertWeatherGridCellsToWeatherMap(metric)).thenReturn(layers);

        WeatherMap result = blockingStub.findFilteredGridDataBySpatialBox(validRequest);

        assertNotNull(result);
        assertEquals(15, result.getIntervalMinutes());
        assertEquals(1710000000L, result.getTimestampBucket());
        assertEquals(layers, result.getGridCellsOrThrow("v123"));
    }

    @ParameterizedTest
    @MethodSource("provideInvalidRequests")
    void shouldReturn400InvalidArgument_whenConstraintsAreViolated(InvalidRequestTestCase testCase) {
        SpatialBoxRequest invalidRequest = testCase.requestBuilder.build();

        StatusRuntimeException exception = assertThrows(StatusRuntimeException.class, () -> blockingStub.findFilteredGridDataBySpatialBox(invalidRequest));

        assertNotNull(exception);
        assertEquals(Status.Code.INVALID_ARGUMENT, exception.getStatus().getCode());
        assertNotNull(exception.getStatus().getDescription());
        assertTrue(
                exception.getStatus().getDescription().contains(testCase.expectedErrorMessageFragment),
                () -> "Validation mismatch!\n"+
                        "-> Actual description:   \""+exception.getStatus().getDescription()+"\"\n"+
                        "-> Expected to contain:  \""+testCase.expectedErrorMessageFragment+"\""
        );
    }

    @Test
    void shouldThrowNotFoundException_whenNoWeatherMapBucketExistsInDatabase() {
        SpatialBoxRequest missingBucketRequest = createValidBase().setTimestampBucket(1810000000L).setTimeIntervalInMinutes(30).build();
        when(historicalQueryRepository.findByTimestampBucketAndIntervalMinutes(1810000000L, 30)).thenReturn(Optional.empty());

        StatusRuntimeException exception = assertThrows(StatusRuntimeException.class, () -> blockingStub.findFilteredGridDataBySpatialBox(missingBucketRequest));

        assertEquals(Status.Code.NOT_FOUND, exception.getStatus().getCode());
    }

    @Test
    void shouldThrowNotFoundException_whenMetricsCollectionIsEmpty() {
        SpatialBoxRequest emptyMetricsRequest = createValidBase().setMinLat(10.0).setMaxLat(20.0).setMinLon(30.0).setMaxLon(40.0).build();
        UUID emptyBucketId = UUID.randomUUID();
        WeatherMapBucket mockBucket = mock(WeatherMapBucket.class);
        when(mockBucket.getId()).thenReturn(emptyBucketId);
        when(historicalQueryRepository.findByTimestampBucketAndIntervalMinutes(1710000000L, 15)).thenReturn(Optional.of(mockBucket));
        when(historicalQueryRepository.findByBucketIdAndSpatialBox(emptyBucketId, 10.0, 20.0, 30.0, 40.0)).thenReturn(Collections.emptyList());

        StatusRuntimeException exception = assertThrows(StatusRuntimeException.class, () -> blockingStub.findFilteredGridDataBySpatialBox(emptyMetricsRequest));

        assertEquals(Status.Code.NOT_FOUND, exception.getStatus().getCode());
    }

    @Test
    void shouldThrowInternalException_whenDatabaseThrowsUnexpectedRuntimeException() {
        SpatialBoxRequest crashingRequest = createValidBase().build();
        when(historicalQueryRepository.findByTimestampBucketAndIntervalMinutes(1710000000L, 15)).thenThrow(new RuntimeException("Database connectivity lost"));

        StatusRuntimeException exception = assertThrows(StatusRuntimeException.class, () -> blockingStub.findFilteredGridDataBySpatialBox(crashingRequest));

        assertEquals(Status.Code.INTERNAL, exception.getStatus().getCode());
    }

    private GrpcAdviceExceptionHandler configureAdviceHandler(org.springframework.context.ApplicationContext context) {
        GrpcAdviceDiscoverer discoverer = new GrpcAdviceDiscoverer(context) {
            @Override
            public Map<String, Object> getAnnotatedBeans() {
                Map<String, Object> unmarshalledBeans = new HashMap<>();
                super.getAnnotatedBeans().forEach((name, bean) ->
                        unmarshalledBeans.put(name, AopProxyUtils.getSingletonTarget(bean)!=null?AopProxyUtils.getSingletonTarget(bean):bean)
                );
                return unmarshalledBeans;
            }
        };
        discoverer.afterPropertiesSet();
        GrpcExceptionHandlerMethodResolver methodResolver = new GrpcExceptionHandlerMethodResolver(discoverer);
        methodResolver.afterPropertiesSet();
        return new GrpcAdviceExceptionHandler(methodResolver);
    }

    private ServerInterceptor createAdviceInterceptor(GrpcAdviceExceptionHandler adviceHandler) {
        return new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                    ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
                return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(next.startCall(call, headers)) {
                    @Override
                    public void onHalfClose() {
                        try {
                            super.onHalfClose();
                        } catch(Throwable ex) {
                            StatusException statusException = adviceHandler.handleException(ex);
                            if(statusException!=null) {
                                call.close(statusException.getStatus(), statusException.getTrailers()!=null?statusException.getTrailers():new Metadata());
                            } else {
                                call.close(Status.INTERNAL.withCause(ex), new Metadata());
                            }
                        }
                    }
                };
            }
        };
    }

    private void mockDefaultDatabaseBehavior() {
        WeatherMapBucket defaultMockBucket = mock(WeatherMapBucket.class);
        when(defaultMockBucket.getId()).thenReturn(UUID.randomUUID());
        when(historicalQueryRepository.findByTimestampBucketAndIntervalMinutes(anyLong(), anyInt()))
                .thenReturn(Optional.of(defaultMockBucket));
    }

    private SpatialBoxRequest.Builder createValidBase() {
        return SpatialBoxRequest.newBuilder()
                .setTimestampBucket(1710000000L)
                .setTimeIntervalInMinutes(15)
                .setMinLat(45.0)
                .setMaxLat(46.0)
                .setMinLon(12.0)
                .setMaxLon(13.0);
    }

    record InvalidRequestTestCase(SpatialBoxRequest.Builder requestBuilder, String expectedErrorMessageFragment) {
    }

    static class TestConfig {
        @Bean
        public HistoricalQueryRepository historicalQueryRepository() {
            return mock(HistoricalQueryRepository.class);
        }

        @Bean
        public HistoricalDataConvertService weatherMapConverter() {
            return mock(HistoricalDataConvertService.class);
        }

        @Bean
        public HistoricalExternalCommunicationObserver historicalExternalCommunicationObserver(HistoricalQueryRepository queryRepo, HistoricalDataConvertService converter) {
            return new HistoricalExternalCommunicationObserver(queryRepo, converter);
        }

        @Bean
        public GlobalPlatformGrpcAdviceEngine globalPlatformGrpcAdviceEngine() {
            return new GlobalPlatformGrpcAdviceEngine();
        }
    }
}