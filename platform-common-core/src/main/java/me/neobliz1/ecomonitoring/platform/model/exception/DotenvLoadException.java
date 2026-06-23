package me.neobliz1.ecomonitoring.platform.model.exception;

import static me.neobliz1.ecomonitoring.platform.model.exception.ErrorCode.ENV_FILE_LOAD_FAILED;

public class DotenvLoadException extends BasePlatformException {

    public DotenvLoadException(Throwable cause) {
        super(ENV_FILE_LOAD_FAILED);
        initCause(cause);
    }
}
