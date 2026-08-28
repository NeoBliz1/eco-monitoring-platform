package me.neobliz1.ecomonitoring.platform.config;

import io.github.cdimascio.dotenv.Dotenv;
import io.github.cdimascio.dotenv.DotenvEntry;
import lombok.NonNull;
import me.neobliz1.ecomonitoring.platform.model.exception.DotenvLoadException;
import org.springframework.boot.EnvironmentPostProcessor;
import org.springframework.boot.SpringApplication;
import org.springframework.core.Ordered;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class DevDotenvEnvironmentPostProcessor implements EnvironmentPostProcessor, Ordered {

    private static final String PROPERTY_SOURCE_NAME = "devDotenvProperties";
    private static final String ENV_FILE_NAME = ".env";
    private static final String CREDENTIALS_ENV_FILE_NAME = "dev_creds.env";

    @Override
    public void postProcessEnvironment(@NonNull ConfigurableEnvironment environment, @NonNull SpringApplication application) {
        String activeProfiles = environment.getProperty("spring.profiles.active");
        if(activeProfiles==null) {
            return;
        }
        boolean devIsActive = Arrays.stream(activeProfiles.split(","))
                .map(String::trim)
                .anyMatch(profile -> profile.equals("dev"));
        if(!devIsActive) {
            return;
        }
        Map<String, Object> dotenvMap = new HashMap<>();
        loadEnvFile(ENV_FILE_NAME, dotenvMap);
        loadEnvFile(CREDENTIALS_ENV_FILE_NAME, dotenvMap);
        if(!dotenvMap.isEmpty()) {
            environment.getPropertySources().addLast(
                    new MapPropertySource(PROPERTY_SOURCE_NAME, dotenvMap)
            );
        }
    }

    private void loadEnvFile(String filename, Map<String, Object> targetMap) {
        File file = new File(filename);
        if(!file.exists()) {
            return;
        }
        try {
            Dotenv dotenv = Dotenv.configure()
                    .filename(filename)
                    .ignoreIfMalformed()
                    .load();
            for(DotenvEntry entry : dotenv.entries()) {
                targetMap.put(entry.getKey(), entry.getValue());
            }
        } catch(Exception e) {
            throw new DotenvLoadException("Failed to load or parse environment configurations from "+filename, e);
        }
    }

    @Override
    public int getOrder() {
        return HIGHEST_PRECEDENCE+10;
    }
}

