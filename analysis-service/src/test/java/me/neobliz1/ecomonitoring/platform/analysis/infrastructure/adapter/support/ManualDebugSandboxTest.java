package me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.support;

import static me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.support.IntegrationTestSupport.getComposeContainer;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.ComposeContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.concurrent.TimeUnit;

@Slf4j
@Testcontainers
@Disabled("Manual local debugging scratchpad only")
public class ManualDebugSandboxTest {

    @Container
    @SuppressWarnings("unused")
    public static final ComposeContainer ENVIRONMENT = getComposeContainer();

    @Test
    void bootTestContextForManualDebugging() {
        try {
            log.info("Debugger sandbox is active.");
            TimeUnit.HOURS.sleep(1);
        } catch(InterruptedException e) {
            log.info("Debug session manually terminated or interrupted.");
            Thread.currentThread().interrupt();
        }
    }
}