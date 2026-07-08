package me.neobliz1.ecomonitoring.platform.model.exception;

import lombok.Getter;

@Getter
public class BasePlatformException extends RuntimeException {
    private final ErrorCode errorCode;

    public BasePlatformException(ErrorCode errorCode) {
        super();
        this.errorCode = errorCode;
    }

    public BasePlatformException(String msg, ErrorCode errorCode) {
        super(msg);
        this.errorCode = errorCode;
    }
}