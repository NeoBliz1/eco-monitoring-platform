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

    public static final String RESOLVED_HOST = "127.0.0.1";
    public static final String DEVELOPMENT_PROFILE = "dev";

    public ServiceAddressRecord discoverServiceAddressFromConsulServerByName(DiscoveryClient discoveryClient,
                                                                             ConfigurableEnvironment environment,
                                                                             String serviceName) {
        List<ServiceInstance> instances = discoveryClient.getInstances(serviceName);

        String resolvedHost = RESOLVED_HOST;
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

    public List<String> discoverServiceAddressesFromConsulServerByName(DiscoveryClient discoveryClient,
                                                                       ConfigurableEnvironment environment,
                                                                       String serviceName) {
        return discoveryClient.getInstances(serviceName).stream()
                .map(instance -> {
                    String host = RESOLVED_HOST;
                    if(!environment.acceptsProfiles(Profiles.of(DEVELOPMENT_PROFILE))) {
                        host = instance.getHost();
                    }
                    return host+":"+instance.getPort();
                })
                .toList();
    }

    public record ServiceAddressRecord(String resolvedHost, int resolvedPort) {
    }
}
