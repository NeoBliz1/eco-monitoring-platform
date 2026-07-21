package me.neobliz1.ecomonitoring.platform.common.api.uri;

public interface UriConstant {

    // Analysis service
    String WEATHER_MAP_URI = "/api/v1/weather-map";
    String LATEST_WEATHER_MAP_ENDPOINT = "/latest";

    // Ingestion service
    String TELEMETRY_URI = "/api/v1/telemetry";
    String REACTIVE_TELEMETRY_ENDPOINT_URI = "/mono";
    String BLOCKING_TELEMETRY_ENDPOINT_URI = "/virtual";
}