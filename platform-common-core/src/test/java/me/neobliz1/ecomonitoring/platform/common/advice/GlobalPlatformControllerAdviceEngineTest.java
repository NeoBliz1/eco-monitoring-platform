package me.neobliz1.ecomonitoring.platform.common.advice;

import static me.neobliz1.ecomonitoring.platform.model.exception.EcoPlatformErrorCode.PIPELINE_TIMEOUT;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

import jakarta.validation.ConstraintViolationException;
import me.neobliz1.ecomonitoring.platform.model.exception.BasePlatformException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.context.MessageSource;
import org.springframework.http.HttpStatus;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.Collections;
import java.util.Locale;


@ExtendWith(MockitoExtension.class)
class GlobalPlatformControllerAdviceEngineTest {

    @Mock
    private MessageSource messageSource;

    @InjectMocks
    private GlobalPlatformControllerAdviceEngine adviceEngine;

    private MockMvc mockMvc;

    @Test
    void shouldSuccessReturnErrorEnvelopeDto_whenThrowBasePlatformException() throws Exception {
        setupMockMvcForController(new TestDummyController());
        String expectedExDescription = "Vector sidecar processing deadline exceeded. Local backpressure disk buffer ring engagement required.";
        String errCode = PIPELINE_TIMEOUT.getCodeStr();
        when(messageSource.getMessage(anyString(), any(), any(Locale.class)))
                .thenReturn(expectedExDescription);


        performTest(get("/test-base-error"), 408, errCode, expectedExDescription);
    }

    @Test
    void shouldSuccessReturnErrorEnvelopeDto_whenThrowRuntimeException() throws Exception {
        setupMockMvcForController(new TestRuntimeController());
        String expectedExDescription = "Exception occurred: Some reason";

        performTest(get("/test-runtime-error"), 500, HttpStatus.INTERNAL_SERVER_ERROR.toString(), expectedExDescription);
    }

    @Test
    void shouldSuccessReturnErrorEnvelopeDto_whenThrowConstraintViolationException() throws Exception {
        setupMockMvcForController(new TestConstraintController());
        String expectedExDescription = "Invalid request parameters: throwError.someInt: must be greater than or equal to 1";

        performTest(get("/test-constraint-error"), 400, HttpStatus.BAD_REQUEST.toString(), expectedExDescription);
    }

    @Test
    void shouldSuccessReturnErrorEnvelopeDto_whenThrowTypeMismatchException() throws Exception {
        setupMockMvcForController(new TestTypeMismatchController());
        String expectedExDescription = "Invalid request parameter value. Parameter 'someInt' expects type 'int' but received value: 'invalid-value'";

        performTest(get("/test-mismatch-error").param("someInt", "invalid-value"), 400, HttpStatus.BAD_REQUEST.toString(), expectedExDescription);
    }

    private void setupMockMvcForController(Object controller) {
        this.mockMvc = MockMvcBuilders.standaloneSetup(controller)
                .setControllerAdvice(adviceEngine)
                .build();
    }

    private void performTest(MockHttpServletRequestBuilder requestBuilder, int expectedStatus, String expectedCode, String expectedDescription) throws Exception {
        mockMvc.perform(requestBuilder)
                .andExpect(status().is(expectedStatus))
                .andExpect(jsonPath("$.code").value(expectedCode))
                .andExpect(jsonPath("$.description").value(expectedDescription))
                .andExpect(jsonPath("$.timestamp").value(greaterThanOrEqualTo(Instant.now().toEpochMilli()-200)));
    }

    private static class DummyPlatformException extends BasePlatformException {
        public DummyPlatformException() {
            super(PIPELINE_TIMEOUT);
        }
    }

    @RestController
    private static class TestDummyController {
        @GetMapping("/test-base-error")
        public void throwError() {
            throw new DummyPlatformException();
        }
    }

    @RestController
    private static class TestRuntimeController {
        @GetMapping("/test-runtime-error")
        public void throwError() {
            throw new RuntimeException("Some reason");
        }
    }

    @RestController
    private static class TestConstraintController {
        @GetMapping("/test-constraint-error")
        public void throwError() {
            throw new ConstraintViolationException("throwError.someInt: must be greater than or equal to 1", Collections.emptySet());
        }
    }

    @RestController
    private static class TestTypeMismatchController {
        @SuppressWarnings("unused")
        @GetMapping("/test-mismatch-error")
        public void throwError(@RequestParam("someInt") int someInt) {
        }
    }
}

