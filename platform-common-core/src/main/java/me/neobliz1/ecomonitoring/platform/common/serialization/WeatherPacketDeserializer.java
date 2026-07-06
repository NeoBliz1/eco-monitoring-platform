package me.neobliz1.ecomonitoring.platform.common.serialization;

import com.google.protobuf.InvalidProtocolBufferException;
import me.neobliz1.ecomonitoring.platform.model.exception.WeatherPacketDeserializationException;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.WeatherPacket;
import org.apache.kafka.common.serialization.Deserializer;

public class WeatherPacketDeserializer implements Deserializer<WeatherPacket> {

    @Override
    public WeatherPacket deserialize(String topic, byte[] data) {
        if(data==null || data.length==0) {
            return null;
        }
        try {
            return WeatherPacket.parseFrom(data);
        } catch(InvalidProtocolBufferException e) {
            throw new WeatherPacketDeserializationException(e);
        }
    }
}

