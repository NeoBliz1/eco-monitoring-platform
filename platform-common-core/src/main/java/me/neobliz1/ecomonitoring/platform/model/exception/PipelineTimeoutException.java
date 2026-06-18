package me.neobliz1.ecomonitoring.platform.model.exception;

import static me.neobliz1.ecomonitoring.platform.model.exception.ErrorCode.PIPELINE_TIMEOUT;

public class PipelineTimeoutException extends BasePlatformException {

    public PipelineTimeoutException() {
        super(PIPELINE_TIMEOUT);
    }
}
