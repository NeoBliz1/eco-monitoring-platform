package me.neobliz1.ecomonitoring.platform.model.exception;

import static me.neobliz1.ecomonitoring.platform.model.exception.EcoPlatformErrorCode.SERVICE_INSTANCE_NOT_FOUND;

public class ServiceInstanceNotFoundException extends BasePlatformException {

    public ServiceInstanceNotFoundException(String serviceName) {
        super(String.format("No service %s instance found", serviceName), SERVICE_INSTANCE_NOT_FOUND);
    }
}
