package me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

@Slf4j
@Disabled("Manual local debugging scratchpad only")
public class ManualDebugSandboxTest extends ManualDebugSandboxTestSupport {

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