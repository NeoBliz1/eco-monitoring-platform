package me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter;

import static me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter.IntegrationTestSupport.DOCKER_HISTORY_TEST_DOCKER_COMPOSE_YAML;
import static me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter.IntegrationTestSupport.PG_DB;
import static me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter.IntegrationTestSupport.PG_DB_PORT;
import static me.neobliz1.ecomonitoring.platform.test.common.util.WeatherTestUtils.loadEnvironmentMap;

import lombok.extern.slf4j.Slf4j;
import org.testcontainers.containers.ComposeContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.File;

@Slf4j
@Testcontainers
public abstract class ManualDebugSandboxTestSupport {

    @Container
    public static final ComposeContainer ENVIRONMENT;

    static {
        ENVIRONMENT = new ComposeContainer(new File(DOCKER_HISTORY_TEST_DOCKER_COMPOSE_YAML))
                .withEnv(loadEnvironmentMap())
                .withExposedService(PG_DB, PG_DB_PORT)
                .withRemoveVolumes(true)
                .withTailChildContainers(true);
    }
}