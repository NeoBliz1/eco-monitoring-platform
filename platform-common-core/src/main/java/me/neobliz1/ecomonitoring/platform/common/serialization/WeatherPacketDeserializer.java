package me.neobliz1.ecomonitoring.platform.common.serialization;

import com.google.protobuf.InvalidProtocolBufferException;
import lombok.extern.slf4j.Slf4j;
import me.neobliz1.ecomonitoring.platform.model.exception.WeatherPacketDeserializationException;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.WeatherPacket;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.charset.StandardCharsets;

@Slf4j
public class WeatherPacketDeserializer implements Deserializer<WeatherPacket> {

    @Override
    public WeatherPacket deserialize(String topic, byte[] data) {
        String rawMessageString = new String(data, StandardCharsets.UTF_8);
        if(data.length==0 || rawMessageString.contains("<null>")) {
            log.warn("Skipping unparseable leftover string record: {}", rawMessageString);
            return null;
        }
        try {
            return WeatherPacket.parseFrom(data);
        } catch(InvalidProtocolBufferException e) {
            throw new WeatherPacketDeserializationException(e);
        }
    }
}

