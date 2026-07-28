package me.neobliz1.ecomonitoring.platform.ingestion.infrastructure.outbound.grpc.vector;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import com.google.common.base.Ticker;
import com.google.protobuf.ByteString;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaResponse;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import me.neobliz1.ecomonitoring.platform.ingestion.infrastructure.adapter.outbound.grpc.vector.TelemetryPayloadMapper;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.WeatherPacket;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;
import vector.PushEventsRequest;

import java.io.IOException;

@ExtendWith(MockitoExtension.class)
public class TelemetryPayloadMapperTest {

    @Mock
    private SchemaRegistryClient mockSchemaRegistryClient;
    @InjectMocks
    private TelemetryPayloadMapper telemetryPayloadMapper;

    @BeforeEach
    void setUp() throws RestClientException, IOException {
        ReflectionTestUtils.setField(telemetryPayloadMapper, "schemaRegistryUrl", "http://localhost:8085");
        ReflectionTestUtils.setField(telemetryPayloadMapper, "kafkaIngestionLiveTopic", "environment.weather.telemetry.live");
        ReflectionTestUtils.setField(telemetryPayloadMapper, "schemaRegistryClient", mockSchemaRegistryClient);
        when(mockSchemaRegistryClient.ticker()).thenReturn(Ticker.systemTicker());
        RegisterSchemaResponse mockResponse = new RegisterSchemaResponse();
        mockResponse.setId(1);
        mockResponse.setVersion(1);
        mockResponse.setSchema("syntax = \"proto3\"; message Mock {}");
        when(mockSchemaRegistryClient.registerWithResponse(
                anyString(),
                any(ParsedSchema.class),
                anyBoolean(),
                anyBoolean()
        )).thenReturn(mockResponse);
    }

    @Test
    void shouldReturnVectorRequest_whenReceivedValidWeatherPacket() {
        WeatherPacket originalPacket = WeatherPacket.newBuilder()
                .setStationId("1")
                .build();
        byte[] expectedProtobufBytes = new byte[]{ 0, 0, 0, 0, 1, 0, 10, 1, 49 };

        PushEventsRequest pushRequest = telemetryPayloadMapper.toPushRequest(originalPacket);
        var fieldsMap = pushRequest.getEvents(0)
                .getLog()
                .getValue()
                .getMap()
                .getFieldsMap();
        ByteString actualRawBytes = fieldsMap.get("raw_protobuf_packet").getRawBytes();

        assertNotNull(actualRawBytes);
        assertNotNull(pushRequest);
        assertEquals(1, pushRequest.getEventsCount());
        assertEquals(9, actualRawBytes.size());
        assertArrayEquals(expectedProtobufBytes, actualRawBytes.toByteArray());
    }
}
