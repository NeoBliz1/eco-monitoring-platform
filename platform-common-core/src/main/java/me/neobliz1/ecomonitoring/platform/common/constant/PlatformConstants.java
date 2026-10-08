package me.neobliz1.ecomonitoring.platform.common.constant;

import lombok.experimental.UtilityClass;

@UtilityClass
public class PlatformConstants {

    // Platform profiles
    public final static String TX_CHAIN_CONFIRMATION_PROFILE = "weather-packet-chain-confirmation";
    public final static String LOCAL_PROFILE = "local";
    public final static String DEV_PROFILE = "dev";
    public final static String COMMON_PROFILE = "common";
    public final static String TRACE_PROFILE = "trace";

    // Platform traces span
    public static final String ANALYSIS_WEATHER_TOPOLOGY_TRACER = "Analysis_Weather_Topology";
    public static final String ANALYSIS_FLUSH_AGGREGATION_WINDOW_SPAN = "Analysis_Flush_Aggregation_Window";
    public static final String ANALYSIS_AGGREGATE_SPAN = "Analysis_Aggregate_Record";
    public static final String ANALYSIS_DEDUPLICATE_SPAN = "Analysis_Deduplicate_Record";
    public static final String HISTORICAL_KAFKA_LISTENER_TRACER = "Historical_WeatherMap_Kafka_Listener_Tracer";
    public static final String HISTORICAL_WEATHER_MAP_CONVERTER_TRACER = "Historical_WeatherMap_Converter_Tracer";
    public static final String HISTORICAL_WEATHER_MAP_CONSUMER_MERGE_TELEMETRY = "Historical_Weather_Map_Consumer_Merge_Telemetry";

    // Common constants
    public final static String SCHEMA_REGISTRY = "schema-registry";
    public final static String SCHEMA_REGISTRY_URL = "schema.registry.url";
    public static final String SPRING_SCHEMA_REGISTRY_URL_PROP_NAME = "spring.kafka.streams.properties.schema.registry.url";
    public final static String RAW_PROTOBUF_PACKET = "raw_protobuf_packet";
    public final static String GEOHASH_SEPARATOR = "#";
    public static final String TRACE_PARENT_FORMAT = "00-%s-%s-%s";
}