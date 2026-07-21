package me.neobliz1.ecomonitoring.platform.common.util;

import static me.neobliz1.ecomonitoring.platform.common.constant.PlatformConstants.SCHEMA_REGISTRY;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import me.neobliz1.ecomonitoring.platform.model.exception.ServiceInstanceNotFoundException;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.Profiles;

import java.util.List;
import java.util.Map;

@Slf4j
@UtilityClass
public class PlatformCommonUtils {

    public static final String LOCAL_HOST = "127.0.0.1";
    public static final String DEVELOPMENT_PROFILE = "dev";

    public static ServiceAddressRecord discoverServiceAddressFromConsulServerByName(DiscoveryClient discoveryClient,
                                                                             ConfigurableEnvironment environment,
                                                                             String serviceName) {
        List<ServiceInstance> instances = discoveryClient.getInstances(serviceName);

        String resolvedHost = LOCAL_HOST;
        int resolvedPort;

        if(!instances.isEmpty()) {
            ServiceInstance redisInstance = instances.getFirst();
            if(!environment.acceptsProfiles(Profiles.of(DEVELOPMENT_PROFILE))) {
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

        boolean isDevelopment = environment.acceptsProfiles(Profiles.of(DEVELOPMENT_PROFILE));

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
                        Map.of("spring.kafka.streams.properties.schema.registry.url", schemaRegistryUrl))
        );
        log.info("Consul dynamically routed Schema registry to: {}", schemaRegistryUrl);
    }

    public record ServiceAddressRecord(String resolvedHost, int resolvedPort) {
    }
}
