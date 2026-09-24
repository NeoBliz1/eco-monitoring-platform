package me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter;

import static me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter.IntegrationTestSupport.DOCKER_HISTORY_TEST_DOCKER_COMPOSE_YAML;
import static me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter.IntegrationTestSupport.PG_DB;
import static me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter.IntegrationTestSupport.PG_DB_PORT;
import static me.neobliz1.ecomonitoring.platform.test.common.util.WeatherTestUtils.loadEnvironmentMap;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.ComposeContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.File;
import java.util.concurrent.TimeUnit;

@Slf4j
@Testcontainers
@Disabled("Manual local debugging scratchpad only")
public class ManualDebugSandboxTest {

    @Container
    public static final ComposeContainer ENVIRONMENT;

    static {
        ENVIRONMENT = new ComposeContainer(new File(DOCKER_HISTORY_TEST_DOCKER_COMPOSE_YAML))
                .withEnv(loadEnvironmentMap())
                .withExposedService(PG_DB, PG_DB_PORT)
                .withRemoveVolumes(true)
                .withTailChildContainers(true);
    }

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