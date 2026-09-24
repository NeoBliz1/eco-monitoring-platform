package me.neobliz1.ecomonitoring.platform.common.constant;

import lombok.experimental.UtilityClass;

@UtilityClass
public class PlatformConstants {

    // Platform profiles
    public final static String TX_CHAIN_CONFIRMATION_PROFILE = "weather-packet-chain-confirmation";
    public final static String LOCAL_PROFILE = "local";
    public final static String DEV_PROFILE = "dev";
    public final static String COMMON_PROFILE = "common";

    // Platform traces span
    public static final String HISTORICAL_WEATHER_MAP_KAFKA_LISTENER_SPAN = "Historical_WeatherMap_Kafka_Listener";
    public static final String KAFKA_STREAMS_FLUSH_AGGREGATION_WINDOW_SPAN = "KafkaStreams_Flush_Aggregation_Window";
    public static final String KAFKA_STREAMS_AGGREGATE_SPAN = "KafkaStreams_Aggregate_Record";
    public static final String KAFKA_STREAMS_DEDUPLICATE_SPAN = "KafkaStreams_Deduplicate_Record";
    public static final String HISTORY_WEATHER_PACKET_TRACE_SPAN = "WeatherMapConverter_MergeTelemetryInBatch";

    public final static String SCHEMA_REGISTRY = "schema-registry";
    public final static String SCHEMA_REGISTRY_URL = "schema.registry.url";
    public static final String SPRING_SCHEMA_REGISTRY_URL_PROP_NAME = "spring.kafka.streams.properties.schema.registry.url";
    public final static String RAW_PROTOBUF_PACKET = "raw_protobuf_packet";
    public final static String GEOHASH_SEPARATOR = "#";
    public static final String TRACE_PARENT_FORMAT = "00-%s-%s-%s";
}