package me.neobliz1.ecomonitoring.platform.common.util;

import static me.neobliz1.ecomonitoring.platform.common.constant.PlatformConstants.SCHEMA_REGISTRY;
import static me.neobliz1.ecomonitoring.platform.common.constant.PlatformConstants.SPRING_SCHEMA_REGISTRY_URL_PROP_NAME;

import io.opentelemetry.context.propagation.TextMapGetter;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import me.neobliz1.ecomonitoring.platform.common.constant.PlatformConstants;
import me.neobliz1.ecomonitoring.platform.model.exception.ServiceInstanceNotFoundException;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.WeatherPacket;
import org.jspecify.annotations.NonNull;
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

    public static @NonNull TextMapGetter<WeatherPacket> getWeatherPacketTextMapGetter() {
        return new TextMapGetter<>() {
            @Override
            public Iterable<String> keys(@NonNull WeatherPacket carrier) {
                return Collections.singletonList("traceparent");
            }

            @Override
            public String get(WeatherPacket carrier, @NonNull String key) {
                if("traceparent".equals(key) && carrier.hasField(WeatherPacket.getDescriptor().findFieldByNumber(5))) {
                    return carrier.getTraceParent();
                }
                return null;
            }
        };
    }

    public static @NonNull TextMapGetter<String> getWeatherMapTextMapGetter() {
        return new TextMapGetter<>() {
            @Override
            public Iterable<String> keys(@NonNull String carrier) {
                return Collections.singletonList("traceparent");
            }

            @Override
            public String get(String carrier, @NonNull String key) {
                return "traceparent".equals(key)?carrier:null;
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
            ServiceInstance redisInstance = instances.getFirst();
            if(!environment.acceptsProfiles(Profiles.of(PlatformConstants.LOCAL_PROFILE))) {
                resolvedHost = redisInstance.getHost();
            }
            resolvedPort = redisInstance.getPort();
        } else {
            throw new ServiceInstanceNotFoundException(serviceName);
        }
        return new ServiceAddressRecord(resolvedHost, resolvedPort);
    }

    public static List<String> discoverServiceAddressesFromConsulServerByName(
            DiscoveryClient discoveryClient,
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

    public record ServiceAddressRecord(String resolvedHost, int resolvedPort) {
    }
}
