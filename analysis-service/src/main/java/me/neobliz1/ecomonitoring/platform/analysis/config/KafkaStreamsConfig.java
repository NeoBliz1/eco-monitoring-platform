package me.neobliz1.ecomonitoring.platform.analysis.config;

import me.neobliz1.ecomonitoring.platform.analysis.service.TelemetryAnalysisService;
import me.neobliz1.ecomonitoring.platform.analysis.service.impl.TelemetryAnalysisServiceImpl;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.WeatherPacket;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.core.KafkaTemplate;

@Configuration
@EnableKafkaStreams
public class KafkaStreamsConfig {

    @Bean
    public TelemetryAnalysisService telemetryAnalysisService(StringRedisTemplate redisTemplate,
                                                             KafkaTemplate<String, byte[]> kafkaTemplate) {
        return new TelemetryAnalysisServiceImpl(redisTemplate, kafkaTemplate);
    }

    @Bean(name = "kafkaStream")
    public KStream<String, WeatherPacket> preventWeatherPacketDuplicationStream(TelemetryAnalysisService telemetryAnalysisService,
                                                                                StreamsBuilder streamsBuilder) {
        return telemetryAnalysisService.buildTopology(streamsBuilder);
    }
}