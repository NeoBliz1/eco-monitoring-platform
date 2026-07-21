package me.neobliz1.ecomonitoring.platform.model.exception;

import static me.neobliz1.ecomonitoring.platform.model.exception.EcoPlatformErrorCode.PROTOCOL_BUFFER_TRANSLATION_ERROR;

public class ProtocolBufferTranslationException extends BasePlatformException {

    public ProtocolBufferTranslationException(String msg, Throwable cause) {
        super(msg, PROTOCOL_BUFFER_TRANSLATION_ERROR);
        initCause(cause);
    }

    public ProtocolBufferTranslationException(Throwable cause) {
        super(PROTOCOL_BUFFER_TRANSLATION_ERROR);
        initCause(cause);
    }
}
