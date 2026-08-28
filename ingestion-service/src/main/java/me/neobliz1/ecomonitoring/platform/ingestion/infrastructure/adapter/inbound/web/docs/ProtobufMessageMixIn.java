package me.neobliz1.ecomonitoring.platform.ingestion.infrastructure.adapter.inbound.web.docs;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true, value = {
        "unknownFields",
        "initialized",
        "parserForType",
        "serializedSize",
        "defaultInstanceForType",
        "allFields",
        "initializationErrorString",
        "descriptorForType",
        "memoizedSerializedSize",
        "stationIdBytes",
        "readingsCount",
        "locationOrBuilder",
        "readingsOrBuilderList",
        "windOrBuilder",
        "ambientOrBuilder",
        "airQualityOrBuilder",
        "precipitationOrBuilder",
        "opticalOrBuilder",
})
public interface ProtobufMessageMixIn {
}