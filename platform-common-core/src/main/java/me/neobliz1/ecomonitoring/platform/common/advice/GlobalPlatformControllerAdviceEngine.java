package me.neobliz1.ecomonitoring.platform.common.advice;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import me.neobliz1.ecomonitoring.platform.model.dto.ErrorEnvelopeDto;
import me.neobliz1.ecomonitoring.platform.model.exception.BasePlatformException;
import org.springframework.context.MessageSource;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

import java.time.Instant;
import java.util.Locale;

@Slf4j
@RequiredArgsConstructor
@ControllerAdvice(basePackages = "me.neobliz1.ecomonitoring.platform")
public class GlobalPlatformControllerAdviceEngine {

    private final MessageSource messageSource;

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorEnvelopeDto> handleGeneralException(Exception ex) {
        String exDescription = "Exception occurred: %s".formatted(ex.getLocalizedMessage());
        log.error("Exception occurred: {}", ex.getLocalizedMessage());

        return new ResponseEntity<>(new ErrorEnvelopeDto(HttpStatus.INTERNAL_SERVER_ERROR.toString(), exDescription,
                Instant.now().toEpochMilli()), HttpStatus.INTERNAL_SERVER_ERROR);
    }

    @ExceptionHandler(BasePlatformException.class)
    public ResponseEntity<ErrorEnvelopeDto> handleBasePlatformException(BasePlatformException ex, Locale locale) {
        String errCode = ex.getErrorCode().getCodeStr();
        String exDescription = messageSource.getMessage(errCode, null, locale);

        log.error("{}: {}", errCode, exDescription);
        String httpStatus = errCode.substring(4, 7);
        return new ResponseEntity<>(new ErrorEnvelopeDto(errCode, exDescription, Instant.now().toEpochMilli()),
                HttpStatus.valueOf(Integer.parseInt(httpStatus)));
    }
}
