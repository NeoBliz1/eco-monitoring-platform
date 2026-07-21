package me.neobliz1.ecomonitoring.platform.model.exception;

import lombok.Getter;

@Getter
public class BasePlatformException extends RuntimeException {
    private final EcoPlatformErrorCode ecoPlatformErrorCode;

    public BasePlatformException(EcoPlatformErrorCode ecoPlatformErrorCode) {
        super();
        this.ecoPlatformErrorCode = ecoPlatformErrorCode;
    }

    public BasePlatformException(String msg, EcoPlatformErrorCode ecoPlatformErrorCode) {
        super(msg);
        this.ecoPlatformErrorCode = ecoPlatformErrorCode;
    }
}