package me.neobliz1.ecomonitoring.platform.model.exception;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public enum ErrorCode {

    PIPELINE_TIMEOUT("ERR-408001"),
    INVALID_STATION_PAYLOAD("ERR-400001"),
    WEATHER_PACKET_DESERIALIZATION_ERROR("ERR-400002"),

    KAFKA_BROKER_DOWN("ERR-500001"),
    REDIS_WINDOW_TIMEOUT("ERR-500002"),
    ENV_FILE_LOAD_FAILED("ERR-500003"),

    REDIS_PASSWORD_NOT_SET("ERR-500004"),
    PROTOCOL_BUFFER_TRANSLATION_ERROR("ERR-500005"),
    SERVICE_INSTANCE_NOT_FOUND("ERR-500006");

    private final String codeStr;
}
