package me.neobliz1.ecomonitoring.platform.ingestion.controller;

import lombok.NonNull;
import org.springframework.context.MessageSourceResolvable;
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
import java.util.stream.Collectors;

public class ReactiveValidationWebExceptionHandler implements WebExceptionHandler {

    @Override
    public @NonNull Mono<Void> handle(@NonNull ServerWebExchange exchange, @NonNull Throwable ex) {
        // Intercept reactive stream validation failures
        if(ex instanceof MethodValidationException ||
                ex instanceof WebExchangeBindException) {

            ServerHttpResponse response = exchange.getResponse();
            response.setStatusCode(HttpStatus.BAD_REQUEST);
            response.getHeaders().setContentType(MediaType.APPLICATION_JSON);

            List<String> errors;

            // Apply your precise diagnostic checking rules
            if(ex instanceof MethodValidationException mve) {
                errors = mve.getParameterValidationResults().stream()
                        .flatMap(result -> result.getResolvableErrors().stream())
                        .map(MessageSourceResolvable::getDefaultMessage)
                        .toList();
            } else {
                WebExchangeBindException wbe = (WebExchangeBindException) ex;
                errors = wbe.getBindingResult().getFieldErrors().stream()
                        .map(error -> error.getField()+": "+error.getDefaultMessage())
                        .toList();
            }

            // Convert the array cleanly to a JSON string array without requiring Jackson mapper dependencies
            String violationsJsonArray = errors.stream()
                    .map(err -> "\""+err.replace("\"", "\\\"")+"\"")
                    .collect(Collectors.joining(",", "[", "]"));

            // Mirror your exact body signature: status, error, violations
            String bodyJson = "{"
                    +"\"status\":400,"
                    +"\"error\":\"Bad Request\","
                    +"\"violations\":"+violationsJsonArray
                    +"}";

            return response.writeWith(Mono.fromSupplier(() -> response.bufferFactory().wrap(bodyJson.getBytes(StandardCharsets.UTF_8))));
        }

        // Pass any other platform infrastructure exceptions straight through the reactive chain
        return Mono.error(ex);
    }
}



