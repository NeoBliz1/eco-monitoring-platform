package me.neobliz1.ecomonitoring.platform.common.api.uri;

import lombok.experimental.UtilityClass;

@UtilityClass
public class UriConstants {

    // API version
    public final static String ECO_PLATFORM_API_VERSION = "/api/v1";

    // Ingestion service
    public final static String TELEMETRY_URI = ECO_PLATFORM_API_VERSION+"/telemetry";
    public final static String REACTIVE_TELEMETRY_ENDPOINT_URI = "/mono";
    public final static String BLOCKING_TELEMETRY_ENDPOINT_URI = "/virtual";

    // Analysis service
    public final static String WEATHER_MAP_URI = ECO_PLATFORM_API_VERSION+"/weather-map";
    public final static String WEATHER_MAP_ENDPOINT = "/spatial";

    // Historical service
    public final static String TX_ID_INGESTION_HISTORY_URI = ECO_PLATFORM_API_VERSION+"/tx-ingestion-history";
    public final static String WEATHER_PACKET_TX_ID_INGESTION = "/weather-packet-tx-id";
}