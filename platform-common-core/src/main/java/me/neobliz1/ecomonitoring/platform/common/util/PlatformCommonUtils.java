package me.neobliz1.ecomonitoring.platform.common.util;

import static me.neobliz1.ecomonitoring.platform.common.constant.PlatformConstants.SCHEMA_REGISTRY;
import static me.neobliz1.ecomonitoring.platform.common.constant.PlatformConstants.SPRING_SCHEMA_REGISTRY_URL_PROP_NAME;

import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.context.propagation.TextMapGetter;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import me.neobliz1.ecomonitoring.platform.common.constant.PlatformConstants;
import me.neobliz1.ecomonitoring.platform.model.exception.ServiceInstanceNotFoundException;
import me.neobliz1.ecomonitoring.platform.model.record.ServiceAddressRecord;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.WeatherPacket;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.WeatherMap;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.Profiles;

import java.util.Collections;
import java.util.List;
import java.util.Map;

@Slf4j
@UtilityClass
public class PlatformCommonUtils {

    public static final String LOCAL_HOST = "127.0.0.1";
    public static final String TRACEPARENT = "traceparent";

    public static @NonNull TextMapGetter<WeatherPacket> getWeatherPacketTextMapGetter() {
        return new TextMapGetter<>() {
            @Override
            public Iterable<String> keys(@NonNull WeatherPacket carrier) {
                return Collections.singletonList(TRACEPARENT);
            }

            @Override
            public @Nullable String get(@Nullable WeatherPacket carrier, @NonNull String key) {
                if(carrier!=null && TRACEPARENT.equals(key) && carrier.hasField(WeatherPacket.getDescriptor().findFieldByNumber(5))) {
                    return carrier.getTraceParent();
                }
                return null;
            }
        };
    }

    public static @NonNull TextMapGetter<WeatherMap> getHeadersTextMapGetter() {
        return new TextMapGetter<>() {
            @Override
            public Iterable<String> keys(@NonNull WeatherMap carrier) {
                return Collections.singletonList(TRACEPARENT);
            }

            @Override
            public @Nullable String get(@Nullable WeatherMap carrier, @NonNull String key) {
                if(carrier!=null && TRACEPARENT.equals(key) && carrier.hasField(WeatherMap.getDescriptor().findFieldByNumber(5))) {
                    return carrier.getTraceParent();
                }
                return null;
            }
        };
    }

    public static ServiceAddressRecord discoverServiceAddressFromConsulServerByName(DiscoveryClient discoveryClient,
                                                                             ConfigurableEnvironment environment,
                                                                             String serviceName) {
        List<ServiceInstance> instances = discoveryClient.getInstances(serviceName);

        String resolvedHost = LOCAL_HOST;
        int resolvedPort;

        if(!instances.isEmpty()) {
            ServiceInstance instance = instances.getFirst();
            if(!environment.acceptsProfiles(Profiles.of(PlatformConstants.LOCAL_PROFILE))) {
                resolvedHost = instance.getHost();
            }
            resolvedPort = instance.getPort();
        } else {
            throw new ServiceInstanceNotFoundException(serviceName);
        }
        return new ServiceAddressRecord(resolvedHost, resolvedPort);
    }

    public static List<String> discoverServiceAddressesFromConsulServerByName(DiscoveryClient discoveryClient,
                                                                              ConfigurableEnvironment environment,
                                                                              String serviceName) {

        List<ServiceInstance> instances = discoveryClient.getInstances(serviceName);

        if(instances.isEmpty()) {
            throw new ServiceInstanceNotFoundException(serviceName);
        }

        boolean isDevelopment = environment.acceptsProfiles(Profiles.of(PlatformConstants.LOCAL_PROFILE));

        return instances.stream()
                .map(instance -> {
                    String host = isDevelopment?LOCAL_HOST:instance.getHost();
                    return host+":"+instance.getPort();
                })
                .toList();
    }

    public static void resolveSchemaRegistryServer(DiscoveryClient discoveryClient, ConfigurableEnvironment environment) {
        ServiceAddressRecord registryRecord = PlatformCommonUtils.discoverServiceAddressFromConsulServerByName(
                discoveryClient, environment, SCHEMA_REGISTRY);
        String schemaRegistryUrl = String.format("http://%s:%s", registryRecord.resolvedHost(), registryRecord.resolvedPort());
        environment.getPropertySources().addFirst(
                new MapPropertySource("consulDynamicSchemaRegistryProps",
                        Map.of(SPRING_SCHEMA_REGISTRY_URL_PROP_NAME, schemaRegistryUrl))
        );
        log.info("Schema registry URL: {}", schemaRegistryUrl);
    }

    public static void addLinksToConsumerSpan(WeatherMap weatherMap, SpanBuilder consumerSpan) {
        if(weatherMap.getTelemetryTraceParentsCount()>0) {
            weatherMap.getTelemetryTraceParentsList().forEach(traceParentStr -> {
                if(traceParentStr!=null && !traceParentStr.isEmpty()) {
                    try {
                        addTraceParentLink(consumerSpan, traceParentStr);
                    } catch(Exception e) {
                        log.warn("Failed to manually link traceparent array index: {}", traceParentStr, e);
                    }
                }
            });
        }
    }

    private static void addTraceParentLink(SpanBuilder consumerSpan, String traceParentStr) {
        String[] parts = traceParentStr.split("-");
        if(parts.length>=4) {
            String traceId = parts[1];
            String spanId = parts[2];

            SpanContext parsedContext =
                    SpanContext.create(
                            traceId,
                            spanId,
                            TraceFlags.getSampled(),
                            TraceState.getDefault()
                    );

            if(parsedContext.isValid()) {
                consumerSpan.addLink(parsedContext);
            }
        }
    }
}
