package me.neobliz1.ecomonitoring.platform.common.constant;

import lombok.experimental.UtilityClass;

@UtilityClass
public class PlatformConstants {

    public final static String SCHEMA_REGISTRY = "schema-registry";
    public final static String SCHEMA_REGISTRY_URL = "schema.registry.url";
    public static final String SPRING_SCHEMA_REGISTRY_URL_PROP_NAME = "spring.kafka.streams.properties.schema.registry.url";
    public final static String RAW_PROTOBUF_PACKET = "raw_protobuf_packet";
    public final static String HASHTAG_DELIMITER = "#";

    // Platform profiles
    public final static String TX_CHAIN_CONFIRMATION_PROFILE = "weather-packet-chain-confirmation";
    public final static String LOCAL_PROFILE = "local";
    public final static String DEV_PROFILE = "dev";
    public final static String COMMON_PROFILE = "common";
}