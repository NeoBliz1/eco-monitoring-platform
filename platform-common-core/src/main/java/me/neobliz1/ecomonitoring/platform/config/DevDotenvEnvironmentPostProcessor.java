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
        File envFile = new File(ENV_FILE_NAME);
        if(!envFile.exists()) {
            return;
        }
        try {
            Dotenv dotenv = Dotenv.configure()
                    .filename(ENV_FILE_NAME)
                    .ignoreIfMalformed()
                    .load();
            Map<String, Object> dotenvMap = new HashMap<>();
            for(DotenvEntry entry : dotenv.entries()) {
                dotenvMap.put(entry.getKey(), entry.getValue());
            }
            if(!dotenvMap.isEmpty()) {
                environment.getPropertySources().addLast(
                        new MapPropertySource(PROPERTY_SOURCE_NAME, dotenvMap)
                );
            }
        } catch(Exception e) {
            throw new DotenvLoadException("Failed to load or parse environment configurations from "+ENV_FILE_NAME, e);
        }
    }

    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE+10;
    }
}

