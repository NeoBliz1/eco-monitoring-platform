package me.neobliz1.ecomonitoring.platform.ingestion.infrastructure.delivery.web;

import lombok.NonNull;
import org.apache.kafka.common.errors.SerializationException;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.validation.method.MethodValidationException;
import org.springframework.web.bind.support.WebExchangeBindException;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebExceptionHandler;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ReactiveValidationWebExceptionHandler implements WebExceptionHandler {

    @Override
    public @NonNull Mono<Void> handle(@NonNull ServerWebExchange exchange, @NonNull Throwable ex) {

        if (shouldHandleException(ex)) {
            ServerHttpResponse response = exchange.getResponse();
            response.setStatusCode(HttpStatus.BAD_REQUEST);
            response.getHeaders().setContentType(MediaType.APPLICATION_JSON);

            List<String> errors = extractErrorMessages(ex);

            String violationsJsonArray = errors.stream()
                    .map(err -> "\"" + (err != null ? err.replace("\"", "\\\"") : "") + "\"")
                    .collect(Collectors.joining(",", "[", "]"));

            String bodyJson = "{"
                    + "\"status\":400,"
                    + "\"error\":\"Bad Request\","
                    + "\"violations\":" + violationsJsonArray
                    + "}";
            byte[] bytes = bodyJson.getBytes(StandardCharsets.UTF_8);

            return response.writeWith(Mono.fromSupplier(() -> response.bufferFactory().wrap(bytes)))
                    .doOnError(err -> DataBufferUtils.release(response.bufferFactory().wrap(bytes)));
        }

        return Mono.error(ex);
    }

    private boolean shouldHandleException(Throwable ex) {
        return ex instanceof MethodValidationException ||
                ex instanceof WebExchangeBindException ||
                ex instanceof SerializationException;
    }

    private List<String> extractErrorMessages(Throwable ex) {
        if (ex == null) {
            return List.of("Unknown error occurred");
        }

        return switch (ex) {
            case MethodValidationException mve -> mve.getParameterValidationResults().stream()
                    .flatMap(result -> result.getResolvableErrors().stream())
                    .map(error -> error.getDefaultMessage() != null ? error.getDefaultMessage() : "Validation failed")
                    .toList();
            case WebExchangeBindException wbe -> wbe.getBindingResult().getFieldErrors().stream()
                    .map(error -> error.getField() + ": " +
                            (error.getDefaultMessage() != null ? error.getDefaultMessage() : "Invalid value"))
                    .toList();
            case SerializationException se -> {
                String baseMsg = se.getMessage() != null ? se.getMessage() : "Serialization failed";
                Throwable cause = se.getCause();
                String causeMsg = (cause != null && cause.getMessage() != null) ? cause.getMessage() : "";
                yield List.of(causeMsg.isEmpty() ? baseMsg : baseMsg + ": " + causeMsg);
            }
            default -> {
                String message = Stream.of(ex.getCause(), ex)
                        .filter(Objects::nonNull)
                        .map(Throwable::getMessage)
                        .filter(Objects::nonNull)
                        .findFirst()
                        .orElseGet(() -> ex.getClass().getSimpleName());
                yield List.of(message);
            }
        };
    }
}