package me.neobliz1.ecomonitoring.platform.model.exception;

import static me.neobliz1.ecomonitoring.platform.model.exception.EcoPlatformErrorCode.L1_CACHE_NOT_AVAILABLE;

public class L1CacheNotAvailableException extends BasePlatformException {

    public L1CacheNotAvailableException(String cacheRegion) {
        super("L1 cache '"+cacheRegion+"' is not available.", L1_CACHE_NOT_AVAILABLE);
    }
}
