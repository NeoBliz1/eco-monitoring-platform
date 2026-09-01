package me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter.outbound.persistence.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import me.neobliz1.ecomonitoring.platform.history.domain.model.entity.WeatherMapBucket;
import me.neobliz1.ecomonitoring.platform.history.domain.port.inbound.HistoricalDataConvertService;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.WeatherMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.UUID;

@ExtendWith(MockitoExtension.class)
class HistoricalPersistenceRepositoryAdapterTest {

    private static final long BUCKET_TIME = 1800000000L;
    private static final int INTERVAL_MINS = 60;

    @Mock
    private HistoricalDataConvertService weatherMapConverter;

    @Mock
    private HistoricalWeatherMapJpaRepository jpaRepository;

    @InjectMocks
    private HistoricalPersistenceRepositoryAdapter adapter;

    @Test
    void shouldPersistTelemetryRecord_whenValidWeatherMapProvided() {
        WeatherMap weatherMap = mock(WeatherMap.class);
        doReturn(BUCKET_TIME).when(weatherMap).getTimestampBucket();
        doReturn(INTERVAL_MINS).when(weatherMap).getIntervalMinutes();
        WeatherMapBucket bucket = new WeatherMapBucket(UUID.randomUUID(), BUCKET_TIME, INTERVAL_MINS);
        doReturn(bucket).when(jpaRepository).upsertBucket(any(UUID.class), eq(BUCKET_TIME), eq(INTERVAL_MINS));

        adapter.persistTelemetryRecord(weatherMap);

        verify(jpaRepository).upsertBucket(any(UUID.class), eq(BUCKET_TIME), eq(INTERVAL_MINS));
        verify(weatherMapConverter).extractTelemetryFromWeatherMap(weatherMap, bucket);
        verify(jpaRepository).saveAndFlush(bucket);
    }

    @Test
    @SuppressWarnings("DataFlowIssue")
    void shouldThrowNullPointerException_whenWeatherMapIsNull() {
        assertThrows(NullPointerException.class, () -> adapter.persistTelemetryRecord(null));

        verifyNoInteractions(jpaRepository, weatherMapConverter);
    }

    @Test
    void shouldThrowRuntimeException_whenUpsertBucketFails() {
        WeatherMap weatherMap = mock(WeatherMap.class);
        doReturn(BUCKET_TIME).when(weatherMap).getTimestampBucket();
        doReturn(INTERVAL_MINS).when(weatherMap).getIntervalMinutes();
        String exceptionMessage = "Database connection failure";
        doThrow(new RuntimeException(exceptionMessage)).when(jpaRepository).upsertBucket(any(UUID.class), eq(BUCKET_TIME), eq(INTERVAL_MINS));

        RuntimeException exception = assertThrows(RuntimeException.class, () -> adapter.persistTelemetryRecord(weatherMap));

        assertEquals(exceptionMessage, exception.getMessage());
        verifyNoInteractions(weatherMapConverter);
        verifyNoMoreInteractions(jpaRepository);
    }

    @Test
    void shouldThrowRuntimeException_whenConverterFailsDuringExtraction() {
        WeatherMap weatherMap = mock(WeatherMap.class);
        doReturn(BUCKET_TIME).when(weatherMap).getTimestampBucket();
        doReturn(INTERVAL_MINS).when(weatherMap).getIntervalMinutes();
        WeatherMapBucket bucket = new WeatherMapBucket(UUID.randomUUID(), BUCKET_TIME, INTERVAL_MINS);
        String exceptionMessage = "Conversion error";
        doReturn(bucket).when(jpaRepository).upsertBucket(any(UUID.class), eq(BUCKET_TIME), eq(INTERVAL_MINS));
        doThrow(new RuntimeException(exceptionMessage)).when(weatherMapConverter).extractTelemetryFromWeatherMap(weatherMap, bucket);

        RuntimeException exception = assertThrows(RuntimeException.class, () -> adapter.persistTelemetryRecord(weatherMap));

        assertEquals(exceptionMessage, exception.getMessage());
    }

    @Test
    void shouldThrowRuntimeException_whenRepositorySaveFailsUnexpectedly() {
        WeatherMap weatherMap = mock(WeatherMap.class);
        doReturn(BUCKET_TIME).when(weatherMap).getTimestampBucket();
        doReturn(INTERVAL_MINS).when(weatherMap).getIntervalMinutes();
        WeatherMapBucket bucket = new WeatherMapBucket(UUID.randomUUID(), BUCKET_TIME, INTERVAL_MINS);
        String exceptionMessage = "Unexpected save failure";
        doReturn(bucket).when(jpaRepository).upsertBucket(any(UUID.class), eq(BUCKET_TIME), eq(INTERVAL_MINS));
        doThrow(new RuntimeException(exceptionMessage)).when(jpaRepository).saveAndFlush(bucket);

        RuntimeException exception = assertThrows(RuntimeException.class, () -> adapter.persistTelemetryRecord(weatherMap));

        assertEquals(exceptionMessage, exception.getMessage());
    }
}
