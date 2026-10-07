package me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter.inbound.grpc;

import static me.neobliz1.ecomonitoring.platform.history.domain.model.constant.HistoricalCacheConstants.BUCKETS_GLOBAL_REGION;
import static me.neobliz1.ecomonitoring.platform.history.domain.model.constant.HistoricalCacheConstants.QUERIES_GLOBAL_REGION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.neobliz1.validproto.config.HttpValidateProtoAutoConfiguration;
import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import me.neobliz1.ecomonitoring.platform.common.advice.GlobalPlatformGrpcAdviceEngine;
import me.neobliz1.ecomonitoring.platform.history.domain.model.dto.WeatherMapBucketCacheDto;
import me.neobliz1.ecomonitoring.platform.history.domain.model.entity.WeatherGridCellLayer;
import me.neobliz1.ecomonitoring.platform.history.domain.model.entity.WeatherMapBucket;
import me.neobliz1.ecomonitoring.platform.history.domain.port.inbound.HistoricalDataConvertService;
import me.neobliz1.ecomonitoring.platform.history.domain.port.outbound.HistoricalQueryRepository;
import me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter.inbound.grpc.record.SpatialQueryKey;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.GridCellLayers;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.WeatherMap;
import org.junit.Rule;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.springframework.aop.framework.AopProxyUtils;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.grpc.server.advice.GrpcAdviceDiscoverer;
import org.springframework.grpc.server.advice.GrpcAdviceExceptionHandler;
import org.springframework.grpc.server.advice.GrpcExceptionHandlerMethodResolver;
import weather.history.HistoryServiceGrpc;
import weather.history.SpatialBoxRequest;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

public class HistoricalExternalCommunicationObserverTest {

    private static final long DEFAULT_TIMESTAMP_BUCKET = 1710000000L;
    private static final int DEFAULT_INTERVAL_MINUTES = 15;
    private static final double DEFAULT_MIN_LAT = 45.0;
    private static final double DEFAULT_MAX_LAT = 46.0;
    private static final double DEFAULT_MIN_LON = 12.0;
    private static final double DEFAULT_MAX_LON = 13.0;
    private static final String DEFAULT_GEOHASH = "v123";

    private static ApplicationContextRunner contextRunner;

    @Rule
    public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

    private Server server;
    private Cache queryCache;
    private Cache bucketCache;
    private HistoricalDataConvertService weatherMapConverter;
    private HistoricalQueryRepository historicalQueryRepository;
    private HistoryServiceGrpc.HistoryServiceBlockingStub blockingStub;

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
                        "timestamp_bucket: must be greater than 0"),
                new InvalidRequestTestCase(
                        SpatialBoxRequest.newBuilder().setTimestampBucket(100).setTimeIntervalInMinutes(0),
                        "time_interval_in_minutes: must be greater than 0"),
                new InvalidRequestTestCase(
                        SpatialBoxRequest.newBuilder().setTimestampBucket(100).setTimeIntervalInMinutes(15).setMinLat(-90.1),
                        "min_lat: must be greater than or equal to -90"),
                new InvalidRequestTestCase(
                        SpatialBoxRequest.newBuilder().setTimestampBucket(100).setTimeIntervalInMinutes(15).setMaxLat(90.1),
                        "max_lat: must be greater than or equal to -90"),
                new InvalidRequestTestCase(
                        SpatialBoxRequest.newBuilder().setTimestampBucket(100).setTimeIntervalInMinutes(15).setMinLon(-180.1),
                        "min_lon: must be greater than or equal to -180"),
                new InvalidRequestTestCase(
                        SpatialBoxRequest.newBuilder().setTimestampBucket(100).setTimeIntervalInMinutes(15).setMaxLon(180.1),
                        "max_lon: must be greater than or equal to -180"));
    }

    @AfterEach
    void tearDown() {
        if(server!=null) {
            server.shutdownNow();
        }
    }

    @BeforeEach
    void setUp() {
        contextRunner.run(context -> {
            queryCache = context.getBean("queryCache", Cache.class);
            bucketCache = context.getBean("bucketCache", Cache.class);
            weatherMapConverter = context.getBean(HistoricalDataConvertService.class);
            historicalQueryRepository = context.getBean(HistoricalQueryRepository.class);
            HistoricalExternalCommunicationObserver serviceImpl =
                    context.getBean(HistoricalExternalCommunicationObserver.class);
            GrpcAdviceExceptionHandler adviceHandler = configureAdviceHandler(context);
            ServerInterceptor adviceInterceptor = createAdviceInterceptor(adviceHandler);
            stubNoPastBuckets();
            stubEmptyCellsForAnyBucket();
            String serverName = InProcessServerBuilder.generateName();
            server = grpcCleanup.register(InProcessServerBuilder.forName(serverName)
                    .directExecutor()
                    .addService(serviceImpl)
                    .intercept(adviceInterceptor)
                    .build()
                    .start());
            blockingStub = HistoryServiceGrpc.newBlockingStub(
                    grpcCleanup.register(InProcessChannelBuilder.forName(serverName).directExecutor().build()));
        });
    }

    @Test
    void shouldReturnWeatherMapLayers_whenPayloadAndSpatialBoxAreValid() {
        SpatialBoxRequest request = buildValidRequest();
        UUID bucketId = UUID.randomUUID();
        WeatherGridCellLayer metric = stubMetric(DEFAULT_GEOHASH, cellLayersOf(5));
        stubBucketInDb(bucketId);
        stubCellsForBucket(bucketId, metric);

        WeatherMap result = blockingStub.findFilteredGridDataBySpatialBox(request);

        assertNotNull(result);
        assertEquals(DEFAULT_INTERVAL_MINUTES, result.getIntervalMinutes());
        assertEquals(DEFAULT_TIMESTAMP_BUCKET, result.getTimestampBucket());
        assertEquals(cellLayersOf(5), result.getGridCellsOrThrow(DEFAULT_GEOHASH));
    }

    @Test
    void shouldReturnWeatherMapDirectlyFromCache_whenQueryKeyWasPreviouslyCached() {
        SpatialBoxRequest request = buildValidRequest();
        GridCellLayers cachedLayers = cellLayersOf(7);
        WeatherMap cachedMap = weatherMapWith(DEFAULT_GEOHASH, cachedLayers);
        stubQueryCacheHit(cachedMap);

        WeatherMap result = blockingStub.findFilteredGridDataBySpatialBox(request);

        assertEquals(cachedMap, result);
        verify(historicalQueryRepository, never())
                .findWeatherBucketByTimestampAndIntervalMinutes(any(SpatialBoxRequest.class));
    }

    @Test
    void shouldSkipBucketLookupInDb_whenBucketIdIsServedFromCache() {
        SpatialBoxRequest request = buildValidRequest();
        UUID cachedBucketId = UUID.randomUUID();
        WeatherGridCellLayer metric = stubMetric(DEFAULT_GEOHASH, cellLayersOf(1));
        stubBucketCacheHit(cachedBucketId);
        stubCellsForBucket(cachedBucketId, metric);

        WeatherMap result = blockingStub.findFilteredGridDataBySpatialBox(request);

        assertNotNull(result);
        assertEquals(1, result.getGridCellsCount());
        verify(historicalQueryRepository, never())
                .findWeatherBucketByTimestampAndIntervalMinutes(any(SpatialBoxRequest.class));
    }

    @Test
    void shouldPopulateQueryCache_whenWeatherMapIsBuiltFromDb() {
        SpatialBoxRequest request = buildValidRequest();
        UUID bucketId = UUID.randomUUID();
        WeatherGridCellLayer metric = stubMetric(DEFAULT_GEOHASH, cellLayersOf(1));
        stubBucketInDb(bucketId);
        stubCellsForBucket(bucketId, metric);

        blockingStub.findFilteredGridDataBySpatialBox(request);

        verify(queryCache).put(any(SpatialQueryKey.class), any(WeatherMap.class));
    }

    @Test
    void shouldPopulateBucketCache_whenBucketIsLoadedFromDb() {
        SpatialBoxRequest request = buildValidRequest();
        UUID bucketId = UUID.randomUUID();
        WeatherMapBucket bucket = buildBucket(bucketId, DEFAULT_TIMESTAMP_BUCKET);
        when(bucket.getIntervalMinutes()).thenReturn(DEFAULT_INTERVAL_MINUTES);
        when(bucket.getVersion()).thenReturn(1L);
        when(historicalQueryRepository.findWeatherBucketByTimestampAndIntervalMinutes(any(SpatialBoxRequest.class)))
                .thenReturn(Optional.of(bucket));

        assertThrows(StatusRuntimeException.class,
                () -> blockingStub.findFilteredGridDataBySpatialBox(request));

        verify(bucketCache, times(4)).put(any(UUID.class), any(WeatherMapBucketCacheDto.class));
    }

    @Test
    void shouldFallBackToPastBucket_whenCurrentBucketHasNoCells() {
        SpatialBoxRequest request = buildValidRequest();
        UUID currentBucketId = UUID.randomUUID();
        UUID pastBucketId = UUID.randomUUID();
        stubBucketInDb(currentBucketId);
        stubCellsForBucket(currentBucketId);
        stubPastBuckets(buildBucket(pastBucketId, DEFAULT_TIMESTAMP_BUCKET-1000));
        WeatherGridCellLayer pastMetric = stubMetric("v999", cellLayersOf(3));
        stubCellsForBucket(pastBucketId, pastMetric);

        WeatherMap result = blockingStub.findFilteredGridDataBySpatialBox(request);

        assertEquals(3, result.getGridCellsOrThrow("v999").getReadingCount());
        verify(historicalQueryRepository)
                .findGridCellLayersByBucketIdAndSpatialBox(eq(pastBucketId), any(SpatialBoxRequest.class));
    }

    @Test
    void shouldStopAtFirstNonEmptyPastBucket_whenMultiplePastBucketsExist() {
        SpatialBoxRequest request = buildValidRequest();
        UUID currentBucketId = UUID.randomUUID();
        UUID firstPastId = UUID.randomUUID();
        UUID secondPastId = UUID.randomUUID();
        stubBucketInDb(currentBucketId);
        stubCellsForBucket(currentBucketId);
        stubPastBuckets(
                buildBucket(firstPastId, DEFAULT_TIMESTAMP_BUCKET-1000),
                buildBucket(secondPastId, DEFAULT_TIMESTAMP_BUCKET-2000));
        WeatherGridCellLayer metric = stubMetric(DEFAULT_GEOHASH, cellLayersOf(1));
        stubCellsForBucket(firstPastId, metric);

        WeatherMap result = blockingStub.findFilteredGridDataBySpatialBox(request);

        assertNotNull(result);
        verify(historicalQueryRepository, never())
                .findGridCellLayersByBucketIdAndSpatialBox(eq(secondPastId), any(SpatialBoxRequest.class));
    }

    @Test
    void shouldKeepFirstLayer_whenTwoMetricsShareTheSameGeohash() {
        SpatialBoxRequest request = buildValidRequest();
        UUID bucketId = UUID.randomUUID();
        WeatherGridCellLayer first = stubMetric(DEFAULT_GEOHASH, cellLayersOf(1));
        WeatherGridCellLayer second = stubMetric(DEFAULT_GEOHASH, cellLayersOf(2));
        stubBucketInDb(bucketId);
        stubCellsForBucket(bucketId, first, second);

        WeatherMap result = blockingStub.findFilteredGridDataBySpatialBox(request);

        assertEquals(cellLayersOf(1), result.getGridCellsOrThrow(DEFAULT_GEOHASH));
    }

    @Test
    void shouldThrowNotFound_whenAllTiersAndPastBucketsAreEmpty() {
        SpatialBoxRequest request = buildValidRequest();
        stubNoBucketInDb();
        stubNoPastBuckets();

        StatusRuntimeException exception = assertThrows(StatusRuntimeException.class,
                () -> blockingStub.findFilteredGridDataBySpatialBox(request));

        assertEquals(Status.Code.NOT_FOUND, exception.getStatus().getCode());
    }

    @Test
    void shouldThrowNotFoundException_whenNoWeatherMapBucketExistsInDatabase() {
        SpatialBoxRequest request = buildValidRequest();
        stubNoBucketInDb();

        StatusRuntimeException exception = assertThrows(StatusRuntimeException.class,
                () -> blockingStub.findFilteredGridDataBySpatialBox(request));

        assertEquals(Status.Code.NOT_FOUND, exception.getStatus().getCode());
    }

    @Test
    void shouldThrowNotFoundException_whenMetricsCollectionIsEmpty() {
        SpatialBoxRequest request = buildValidRequest();
        UUID bucketId = UUID.randomUUID();
        stubBucketInDb(bucketId);
        stubCellsForBucket(bucketId);

        StatusRuntimeException exception = assertThrows(StatusRuntimeException.class,
                () -> blockingStub.findFilteredGridDataBySpatialBox(request));

        assertEquals(Status.Code.NOT_FOUND, exception.getStatus().getCode());
    }

    @Test
    void shouldThrowInternalException_whenDatabaseThrowsUnexpectedRuntimeException() {
        SpatialBoxRequest request = buildValidRequest();
        when(historicalQueryRepository.findWeatherBucketByTimestampAndIntervalMinutes(any(SpatialBoxRequest.class)))
                .thenThrow(new RuntimeException("Database connectivity lost"));

        StatusRuntimeException exception = assertThrows(StatusRuntimeException.class,
                () -> blockingStub.findFilteredGridDataBySpatialBox(request));

        assertEquals(Status.Code.INTERNAL, exception.getStatus().getCode());
    }

    @ParameterizedTest
    @MethodSource("provideInvalidRequests")
    void shouldReturn400InvalidArgument_whenConstraintsAreViolated(InvalidRequestTestCase testCase) {
        SpatialBoxRequest invalidRequest = testCase.requestBuilder.build();

        StatusRuntimeException exception = assertThrows(StatusRuntimeException.class,
                () -> blockingStub.findFilteredGridDataBySpatialBox(invalidRequest));

        assertNotNull(exception);
        assertEquals(Status.Code.INVALID_ARGUMENT, exception.getStatus().getCode());
        assertNotNull(exception.getStatus().getDescription());
        assertTrue(
                exception.getStatus().getDescription().contains(testCase.expectedErrorMessageFragment),
                () -> "Validation mismatch!\n"+
                        "-> Actual description:   \""+exception.getStatus().getDescription()+"\"\n"+
                        "-> Expected to contain:  \""+testCase.expectedErrorMessageFragment+"\"");
    }

    @Test
    void shouldNotifyObserverWithInternalError_whenQueryCacheRegionIsNotRegistered() {
        SpatialBoxRequest request = buildValidRequest();
        CacheManager cacheManagerWithoutQueryRegion = mock(CacheManager.class);
        when(cacheManagerWithoutQueryRegion.getCache(QUERIES_GLOBAL_REGION)).thenReturn(null);
        HistoricalExternalCommunicationObserver observer =
                new HistoricalExternalCommunicationObserver(
                        historicalQueryRepository, weatherMapConverter, cacheManagerWithoutQueryRegion);
        @SuppressWarnings("unchecked")
        StreamObserver<WeatherMap> responseObserver = mock(StreamObserver.class);

        observer.findFilteredGridDataBySpatialBox(request, responseObserver);

        ArgumentCaptor<Throwable> captor = ArgumentCaptor.forClass(Throwable.class);
        verify(responseObserver).onError(captor.capture());
        StatusRuntimeException error = (StatusRuntimeException) captor.getValue();
        assertEquals(Status.Code.INTERNAL, error.getStatus().getCode());
    }

    private void stubQueryCacheHit(WeatherMap cachedMap) {
        when(queryCache.get(any(SpatialQueryKey.class), eq(WeatherMap.class))).thenReturn(cachedMap);
    }

    private void stubBucketCacheHit(UUID bucketId) {
        WeatherMapBucketCacheDto dto =
                new WeatherMapBucketCacheDto(bucketId, DEFAULT_TIMESTAMP_BUCKET, DEFAULT_INTERVAL_MINUTES, 1L);
        when(bucketCache.get(any(UUID.class), eq(WeatherMapBucketCacheDto.class))).thenReturn(dto);
    }

    private void stubBucketInDb(UUID bucketId) {
        WeatherMapBucket bucket = buildBucket(bucketId, DEFAULT_TIMESTAMP_BUCKET);
        when(historicalQueryRepository.findWeatherBucketByTimestampAndIntervalMinutes(any(SpatialBoxRequest.class)))
                .thenReturn(Optional.of(bucket));
    }

    private void stubNoBucketInDb() {
        when(historicalQueryRepository.findWeatherBucketByTimestampAndIntervalMinutes(any(SpatialBoxRequest.class)))
                .thenReturn(Optional.empty());
    }

    private void stubCellsForBucket(UUID bucketId, WeatherGridCellLayer... metrics) {
        when(historicalQueryRepository.findGridCellLayersByBucketIdAndSpatialBox(eq(bucketId), any(SpatialBoxRequest.class)))
                .thenReturn(List.of(metrics));
    }

    private void stubEmptyCellsForAnyBucket() {
        when(historicalQueryRepository.findGridCellLayersByBucketIdAndSpatialBox(any(UUID.class), any(SpatialBoxRequest.class)))
                .thenReturn(List.of());
    }

    private void stubPastBuckets(WeatherMapBucket... buckets) {
        when(historicalQueryRepository.findAllPastBuckets(any(SpatialBoxRequest.class)))
                .thenReturn(List.of(buckets));
    }

    private void stubNoPastBuckets() {
        when(historicalQueryRepository.findAllPastBuckets(any(SpatialBoxRequest.class)))
                .thenReturn(List.of());
    }

    private WeatherGridCellLayer stubMetric(String geohash, GridCellLayers layers) {
        WeatherGridCellLayer metric = mock(WeatherGridCellLayer.class);
        when(metric.getGeohash()).thenReturn(geohash);
        when(weatherMapConverter.convertWeatherGridCellsToWeatherMap(metric)).thenReturn(layers);
        return metric;
    }

    private WeatherMapBucket buildBucket(UUID id, long timestampBucket) {
        WeatherMapBucket bucket = mock(WeatherMapBucket.class);
        when(bucket.getId()).thenReturn(id);
        when(bucket.getTimestampBucket()).thenReturn(timestampBucket);
        return bucket;
    }

    private GridCellLayers cellLayersOf(int readingCount) {
        return GridCellLayers.newBuilder().setReadingCount(readingCount).build();
    }

    private WeatherMap weatherMapWith(String geohash, GridCellLayers layers) {
        return WeatherMap.newBuilder()
                .setIntervalMinutes(DEFAULT_INTERVAL_MINUTES)
                .setTimestampBucket(DEFAULT_TIMESTAMP_BUCKET)
                .putGridCells(geohash, layers)
                .build();
    }

    private SpatialBoxRequest buildValidRequest() {
        return SpatialBoxRequest.newBuilder()
                .setTimestampBucket(DEFAULT_TIMESTAMP_BUCKET)
                .setTimeIntervalInMinutes(DEFAULT_INTERVAL_MINUTES)
                .setMinLat(DEFAULT_MIN_LAT)
                .setMaxLat(DEFAULT_MAX_LAT)
                .setMinLon(DEFAULT_MIN_LON)
                .setMaxLon(DEFAULT_MAX_LON)
                .build();
    }

    private GrpcAdviceExceptionHandler configureAdviceHandler(ApplicationContext context) {
        GrpcAdviceDiscoverer discoverer = new GrpcAdviceDiscoverer(context) {
            @Override
            public Map<String, Object> getAnnotatedBeans() {
                Map<String, Object> unmarshalledBeans = new HashMap<>();
                super.getAnnotatedBeans().forEach((name, bean) ->
                        unmarshalledBeans.put(name,
                                AopProxyUtils.getSingletonTarget(bean)!=null
                                        ?AopProxyUtils.getSingletonTarget(bean)
                                        :bean));
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
                                call.close(statusException.getStatus(),
                                        statusException.getTrailers()!=null
                                                ?statusException.getTrailers()
                                                :new Metadata());
                            } else {
                                call.close(Status.INTERNAL.withCause(ex), new Metadata());
                            }
                        }
                    }
                };
            }
        };
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

        @Bean(name = "queryCache")
        public Cache queryCache() {
            return mock(Cache.class);
        }

        @Bean(name = "bucketCache")
        public Cache bucketCache() {
            return mock(Cache.class);
        }

        @Bean
        public CacheManager springL1CacheManager(Cache queryCache, Cache bucketCache) {
            CacheManager cacheManager = mock(CacheManager.class);
            when(cacheManager.getCache(QUERIES_GLOBAL_REGION)).thenReturn(queryCache);
            when(cacheManager.getCache(BUCKETS_GLOBAL_REGION)).thenReturn(bucketCache);
            return cacheManager;
        }

        @Bean
        public HistoricalExternalCommunicationObserver historicalExternalCommunicationObserver(
                HistoricalQueryRepository queryRepo,
                HistoricalDataConvertService converter,
                CacheManager springL1CacheManager) {
            return new HistoricalExternalCommunicationObserver(queryRepo, converter, springL1CacheManager);
        }

        @Bean
        public GlobalPlatformGrpcAdviceEngine globalPlatformGrpcAdviceEngine() {
            return new GlobalPlatformGrpcAdviceEngine();
        }
    }
}