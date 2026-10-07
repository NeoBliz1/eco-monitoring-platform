package me.neobliz1.ecomonitoring.platform.ingestion.domain.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.NonNull;
import org.apache.kafka.common.errors.SerializationException;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.validation.method.MethodValidationException;
import org.springframework.web.bind.support.WebExchangeBindException;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebExceptionHandler;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;

public class ReactiveValidationWebExceptionHandler implements WebExceptionHandler {

    public static final String VALIDATION_FAILED = "Validation failed";

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public @NonNull Mono<Void> handle(@NonNull ServerWebExchange exchange, @NonNull Throwable ex) {
        if(!shouldHandleException(ex)) {
            return Mono.error(ex);
        }
        ServerHttpResponse response = exchange.getResponse();
        response.setStatusCode(HttpStatus.BAD_REQUEST);
        response.getHeaders().setContentType(MediaType.APPLICATION_JSON);
        DataBuffer buffer = response.bufferFactory().wrap(buildErrorBody(ex));
        return response.writeWith(Mono.just(buffer));
    }

    private boolean shouldHandleException(Throwable ex) {
        return ex instanceof MethodValidationException ||
                ex instanceof WebExchangeBindException ||
                ex instanceof SerializationException;
    }

    private byte[] buildErrorBody(Throwable ex) {
        List<String> violations = extractErrorMessages(ex);
        Map<String, Object> payload = Map.of(
                "status", HttpStatus.BAD_REQUEST.value(),
                "message", VALIDATION_FAILED,
                "error", HttpStatus.BAD_REQUEST.getReasonPhrase(),
                "violations", violations
        );
        try {
            return objectMapper.writeValueAsBytes(payload);
        } catch(JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize validation error body", e);
        }
    }

    private List<String> extractErrorMessages(Throwable ex) {
        if(ex==null) {
            return List.of("Unknown error occurred");
        }
        return switch(ex) {
            case MethodValidationException mve -> extractFromMethodValidation(mve);
            case WebExchangeBindException wbe -> extractFromWebExchangeBind(wbe);
            case SerializationException se -> List.of(joinWithCause(se, "Serialization failed"));
            default -> List.of(joinWithCause(ex, ex.getClass().getSimpleName()));
        };
    }

    private List<String> extractFromMethodValidation(MethodValidationException ex) {
        return ex.getParameterValidationResults().stream()
                .flatMap(result -> result.getResolvableErrors().stream())
                .map(error -> orDefault(error.getDefaultMessage(), VALIDATION_FAILED))
                .toList();
    }

    private List<String> extractFromWebExchangeBind(WebExchangeBindException ex) {
        return ex.getBindingResult().getFieldErrors().stream()
                .map(error -> error.getField()+": "+orDefault(error.getDefaultMessage(), "Invalid value"))
                .toList();
    }

    private String joinWithCause(Throwable ex, String fallback) {
        String base = orDefault(ex.getMessage(), fallback);
        Throwable cause = ex.getCause();
        if(cause==null) {
            return base;
        }
        String causeMsg = cause.getMessage();
        return causeMsg==null?base:base+": "+causeMsg;
    }

    private String orDefault(String value, String fallback) {
        return value!=null?value:fallback;
    }
}