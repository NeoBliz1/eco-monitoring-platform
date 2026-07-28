package me.neobliz1.ecomonitoring.platform.analysis.domain.port.inbound;

import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.WeatherPacket;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

public interface TelemetryAnalysisService {

    KStream<String, WeatherPacket> buildTopology(StreamsBuilder streamsBuilder);
}
