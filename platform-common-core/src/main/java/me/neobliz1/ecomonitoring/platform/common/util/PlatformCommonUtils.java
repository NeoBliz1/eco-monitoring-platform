package me.neobliz1.ecomonitoring.platform.common.util;

import lombok.experimental.UtilityClass;
import me.neobliz1.ecomonitoring.platform.model.exception.ServiceInstanceNotFoundException;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.Profiles;

import java.util.List;

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

    public record ServiceAddressRecord(String resolvedHost, int resolvedPort) {
    }
}
