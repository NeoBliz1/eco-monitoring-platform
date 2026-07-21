package me.neobliz1.ecomonitoring.platform.common.advice;

import static java.util.Optional.ofNullable;
import static me.neobliz1.ecomonitoring.platform.model.exception.EcoPlatformErrorCode.UNKNOW_ERROR_CODE;

import jakarta.validation.ConstraintViolationException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import me.neobliz1.ecomonitoring.platform.model.dto.ErrorEnvelopeDto;
import me.neobliz1.ecomonitoring.platform.model.exception.BasePlatformException;
import me.neobliz1.ecomonitoring.platform.model.exception.EcoPlatformErrorCode;
import org.springframework.context.MessageSource;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

import java.time.Instant;
import java.util.Locale;
import java.util.Objects;

@Slf4j
@RequiredArgsConstructor
@ControllerAdvice(basePackages = "me.neobliz1.ecomonitoring.platform")
public class GlobalPlatformControllerAdviceEngine {

    private final MessageSource messageSource;

    @SuppressWarnings("unused")
    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorEnvelopeDto> handleGeneralException(Exception ex) {
        String exDescription = "Exception occurred: %s".formatted(
                Objects.toString(ex.getLocalizedMessage(), "Unknown error")
        );
        log.error("Exception occurred: {}", ex.getLocalizedMessage());

        return new ResponseEntity<>(new ErrorEnvelopeDto(HttpStatus.INTERNAL_SERVER_ERROR.toString(), exDescription,
                Instant.now().toEpochMilli()), HttpStatus.INTERNAL_SERVER_ERROR);
    }

    @SuppressWarnings("unused")
    @ExceptionHandler(BasePlatformException.class)
    public ResponseEntity<ErrorEnvelopeDto> handleBasePlatformException(BasePlatformException ex, Locale locale) {
        EcoPlatformErrorCode ecoPlatformErrorCode = ofNullable(ex.getEcoPlatformErrorCode()).orElse(UNKNOW_ERROR_CODE);
        String errCode = ecoPlatformErrorCode.getCodeStr();
        String exMsg = ex.getMessage();
        String exDescription = messageSource.getMessage(errCode, null, locale)+(exMsg==null?"":exMsg);

        log.error("{}: {}", errCode, exDescription);
        String httpStatus = errCode.substring(4, 7);
        return new ResponseEntity<>(new ErrorEnvelopeDto(errCode, exDescription, Instant.now().toEpochMilli()),
                HttpStatus.valueOf(Integer.parseInt(httpStatus)));
    }

    @SuppressWarnings("unused")
    @ExceptionHandler(ConstraintViolationException.class)
    public ResponseEntity<ErrorEnvelopeDto> handleConstraintViolation(ConstraintViolationException ex) {
        String exMsg = ex.getMessage();
        String exDescription = "Invalid request parameters: "+(exMsg==null?"":exMsg);

        log.error("{}", exDescription);
        return new ResponseEntity<>(new ErrorEnvelopeDto(HttpStatus.BAD_REQUEST.toString(), exDescription,
                Instant.now().toEpochMilli()), HttpStatus.BAD_REQUEST);
    }

    @SuppressWarnings("unused")
    @ExceptionHandler(MethodArgumentTypeMismatchException.class)
    public ResponseEntity<ErrorEnvelopeDto> handleTypeMismatchException(MethodArgumentTypeMismatchException ex) {
        String paramName = ex.getName();
        String requiredType = ex.getRequiredType()!=null?ex.getRequiredType().getSimpleName():"unknown";
        Object providedValue = ex.getValue();
        String exDescription = "Invalid request parameter value. Parameter '%s' expects type '%s' but received value: '%s'"
                .formatted(paramName, requiredType, providedValue);

        log.error("Type mismatch error: {}", exDescription);
        return new ResponseEntity<>(
                new ErrorEnvelopeDto(HttpStatus.BAD_REQUEST.toString(), exDescription, Instant.now().toEpochMilli()),
                HttpStatus.BAD_REQUEST
        );
    }
}
