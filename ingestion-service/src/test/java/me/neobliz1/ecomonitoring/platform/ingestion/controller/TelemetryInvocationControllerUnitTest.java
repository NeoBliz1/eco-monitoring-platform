package me.neobliz1.ecomonitoring.platform.ingestion.controller;

import static me.neobliz1.ecomonitoring.platform.ingestion.util.TestUtils.STATION_ID;
import static me.neobliz1.ecomonitoring.platform.ingestion.util.TestUtils.createValidBase;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import me.neobliz1.ecomonitoring.platform.ingestion.service.TelemetryIngestionService;
import me.neobliz1.ecomonitoring.platform.model.exception.PipelineTimeoutException;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.WeatherPacket;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import reactor.core.publisher.Mono;

import java.util.Objects;
import java.util.concurrent.TimeoutException;

@ExtendWith(MockitoExtension.class)
public class TelemetryInvocationControllerUnitTest {

    @Mock
    private TelemetryIngestionService telemetryIngestionService;

    @InjectMocks
    private TelemetryInvocationController controller;

    @Captor
    private ArgumentCaptor<WeatherPacket> weatherPacketArgumentCaptor;

    @Test
    void shouldReturn202_whenSendTelemetryRequestToMono() {
        when(telemetryIngestionService.processTelemetryPacket(any())).thenReturn(Mono.just(true));
        WeatherPacket packet = createValidBase().build();

        Mono<ResponseEntity<Void>> responseEntityMono = controller.receivedReactiveSensorStationData(packet);

        assertEquals(HttpStatus.ACCEPTED, Objects.requireNonNull(responseEntityMono.block()).getStatusCode());
        verify(telemetryIngestionService).processTelemetryPacket(weatherPacketArgumentCaptor.capture());
        assertEquals(STATION_ID, weatherPacketArgumentCaptor.getValue().getStationId());
    }

    @Test
    void shouldReturn202_whenSendTelemetryRequestToSingleThreadEndpoint() {
        when(telemetryIngestionService.processTelemetryPacketVirtual(any())).thenReturn(true);
        WeatherPacket packet = createValidBase().build();

        ResponseEntity<Void> responseEntity = controller.receivedSensorStationDataVirtual(packet);

        assertEquals(HttpStatus.ACCEPTED, responseEntity.getStatusCode());
        verify(telemetryIngestionService).processTelemetryPacketVirtual(weatherPacketArgumentCaptor.capture());
        assertEquals(STATION_ID, weatherPacketArgumentCaptor.getValue().getStationId());
    }

    @Test
    void shouldThrowEx_whenSendTelemetryRequestToMono() {
        when(telemetryIngestionService.processTelemetryPacket(any())).thenReturn(Mono.error(TimeoutException::new));
        WeatherPacket packet = createValidBase().build();

        Mono<ResponseEntity<Void>> responseEntityMono = controller.receivedReactiveSensorStationData(packet);

        assertThrows(PipelineTimeoutException.class, () -> Objects.requireNonNull(responseEntityMono.block()));
        verify(telemetryIngestionService).processTelemetryPacket(weatherPacketArgumentCaptor.capture());
        assertEquals(STATION_ID, weatherPacketArgumentCaptor.getValue().getStationId());
    }
}
