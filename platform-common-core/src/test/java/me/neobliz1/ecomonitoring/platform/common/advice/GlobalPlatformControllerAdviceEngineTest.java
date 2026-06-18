package me.neobliz1.ecomonitoring.platform.common.advice;

import static me.neobliz1.ecomonitoring.platform.model.exception.ErrorCode.PIPELINE_TIMEOUT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import me.neobliz1.ecomonitoring.platform.model.dto.ErrorEnvelopeDto;
import me.neobliz1.ecomonitoring.platform.model.exception.BasePlatformException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.context.MessageSource;
import org.springframework.http.HttpStatus;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.time.Instant;

@ExtendWith(MockitoExtension.class)
class GlobalPlatformControllerAdviceEngineTest {

    @Mock
    private MessageSource messageSource;
    private WebTestClient webTestClient;

    private static class DummyPlatformException extends BasePlatformException {
        public DummyPlatformException() {
            super(PIPELINE_TIMEOUT);
        }
    }

    @RestController
    private static class TestDummyController {
        @GetMapping("/test-error")
        public Mono<Void> throwError() {
            return Mono.error(new DummyPlatformException());
        }
    }

    @RestController
    private static class TestRuntimeController {
        @GetMapping("/test-error")
        public Mono<Void> throwError() {
            return Mono.error(new RuntimeException("Some reason"));
        }
    }

    @Test
    void shouldSuccessReturnErrorEnvelopeDto_whenThrowBasePlatformException() {
        GlobalPlatformControllerAdviceEngine adviceEngine = new GlobalPlatformControllerAdviceEngine(messageSource);
        this.webTestClient = WebTestClient.bindToController(new TestDummyController())
                .controllerAdvice(adviceEngine)
                .build();
        String expectedExDescription = "Vector sidecar processing deadline exceeded. Local backpressure disk buffer ring engagement required.";
        String errCode = PIPELINE_TIMEOUT.getCodeStr();
        when(messageSource.getMessage(any(), any(), any())).thenReturn(expectedExDescription);

        webTestClient.get()
                .uri("/test-error")
                .exchange()
                .expectStatus().is4xxClientError()
                .expectBody(ErrorEnvelopeDto.class)
                .consumeWith(result ->
                {
                    ErrorEnvelopeDto responseBody = result.getResponseBody();
                    assertNotNull(responseBody);
                    assertEquals(errCode, responseBody.code());
                    assertEquals(expectedExDescription, responseBody.description());
                    assertTrue(Instant.now().toEpochMilli() >= responseBody.timestamp());
                });
    }

    @Test
    void shouldSuccessReturnErrorEnvelopeDto_whenThrowRuntimeException() {
        GlobalPlatformControllerAdviceEngine adviceEngine = new GlobalPlatformControllerAdviceEngine(messageSource);
        this.webTestClient = WebTestClient.bindToController(new TestRuntimeController())
                .controllerAdvice(adviceEngine)
                .build();
        String expectedExDescription = "Exception occurred: Some reason";

        webTestClient.get()
                .uri("/test-error")
                .exchange()
                .expectStatus().is5xxServerError()
                .expectBody(ErrorEnvelopeDto.class)
                .consumeWith(result ->
                {
                    ErrorEnvelopeDto responseBody = result.getResponseBody();
                    assertNotNull(responseBody);
                    assertEquals(HttpStatus.INTERNAL_SERVER_ERROR.toString(), responseBody.code());
                    assertEquals(expectedExDescription, responseBody.description());
                    assertTrue(Instant.now().toEpochMilli() >= responseBody.timestamp());
                });
    }
}
