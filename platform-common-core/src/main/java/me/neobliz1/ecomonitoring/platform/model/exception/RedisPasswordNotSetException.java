package me.neobliz1.ecomonitoring.platform.model.exception;

import static me.neobliz1.ecomonitoring.platform.model.exception.ErrorCode.REDIS_PASSWORD_NOT_SET;

public class RedisPasswordNotSetException extends BasePlatformException {

    public RedisPasswordNotSetException() {
        super(REDIS_PASSWORD_NOT_SET);
    }
}
