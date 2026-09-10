package me.neobliz1.ecomonitoring.platform.common.constant;

public interface PlatformConstants {

    String SCHEMA_REGISTRY = "schema-registry";
    String SCHEMA_REGISTRY_URL = "schema.registry.url";
    String RAW_PROTOBUF_PACKET = "raw_protobuf_packet";
    String HASHTAG_DELIMITER = "#";

    // Platform profiles
    String TX_CHAIN_CONFIRMATION_PROFILE = "weather-packet-chain-confirmation";
    String LOCAL_PROFILE = "local";
    String DEV_PROFILE = "dev";
    String COMMON_PROFILE = "common";
}