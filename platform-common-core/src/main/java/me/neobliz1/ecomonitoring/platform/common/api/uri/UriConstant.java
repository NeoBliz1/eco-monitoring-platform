package me.neobliz1.ecomonitoring.platform.common.api.uri;

public interface UriConstant {

    // API version
    String ECO_PLATFORM_API_VERSION = "/api/v1";

    // Ingestion service
    String TELEMETRY_URI = ECO_PLATFORM_API_VERSION+"/telemetry";
    String REACTIVE_TELEMETRY_ENDPOINT_URI = "/mono";
    String BLOCKING_TELEMETRY_ENDPOINT_URI = "/virtual";

    // Analysis service
    String WEATHER_MAP_URI = ECO_PLATFORM_API_VERSION+"/weather-map";
    String WEATHER_MAP_ENDPOINT = "/spatial";

    // Historical service
    String TX_ID_INGESTION_HISTORY_URI = ECO_PLATFORM_API_VERSION+"/tx-ingestion-history";
    String WEATHER_PACKET_TX_ID_INGESTION = "/weather-packet-tx-id";
}