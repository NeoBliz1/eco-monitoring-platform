package me.neobliz1.ecomonitoring.platform.common.advice;

import io.grpc.Status;
import io.grpc.StatusException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.MessageSourceResolvable;
import org.springframework.grpc.server.advice.GrpcAdvice;
import org.springframework.grpc.server.advice.GrpcExceptionHandler;
import org.springframework.validation.BindException;
import org.springframework.validation.FieldError;
import org.springframework.validation.method.MethodValidationException;

import java.util.stream.Collectors;

@Slf4j
@GrpcAdvice
public class GlobalPlatformGrpcAdviceEngine {

    @GrpcExceptionHandler(BindException.class)
    public StatusException handleBindException(BindException ex) {
        String validationErrors = ex.getBindingResult().getFieldErrors().stream()
                .map(FieldError::getDefaultMessage)
                .collect(Collectors.joining(", "));

        log.warn("Protobuf model constraint violation intercepted: {}", validationErrors);

        return Status.INVALID_ARGUMENT
                .withDescription("Request validation failed: "+validationErrors)
                .withCause(ex)
                .asException();
    }

    @GrpcExceptionHandler(MethodValidationException.class)
    public StatusException handleMethodValidationException(MethodValidationException ex) {
        String validationErrors = ex.getParameterValidationResults().stream()
                .flatMap(result -> result.getResolvableErrors().stream())
                .map(MessageSourceResolvable::getDefaultMessage)
                .collect(Collectors.joining(", "));
        log.warn("Method invocation validation failure intercepted: {}", validationErrors);
        return Status.INVALID_ARGUMENT
                .withDescription("Request validation failed: "+validationErrors)
                .withCause(ex)
                .asException();
    }

    @GrpcExceptionHandler(Exception.class)
    public StatusException handleUnexpectedException(Exception ex) {
        log.error("Unhandled runtime execution exception intercepted within gRPC context pipeline", ex);

        return Status.INTERNAL
                .withDescription("An unexpected server-side operation failure occurred.")
                .withCause(ex)
                .asException();
    }
}