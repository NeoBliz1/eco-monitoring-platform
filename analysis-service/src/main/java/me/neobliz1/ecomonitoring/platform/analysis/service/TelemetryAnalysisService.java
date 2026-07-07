package me.neobliz1.ecomonitoring.platform.analysis.service;

import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.WeatherPacket;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface TelemetryAnalysisService {

    KStream<String, WeatherPacket> buildTopology(StreamsBuilder streamsBuilder);

    void updateRealTimeSlidingWindow(WeatherPacket packet, double latGrid, double lonGrid);

    void persistAggregatedHistory(Map<Long, Map<String, List<WeatherPacket>>> extractionMatrix);

    Optional<String> getLatestFiveMinuteWeatherMapJson();
}
