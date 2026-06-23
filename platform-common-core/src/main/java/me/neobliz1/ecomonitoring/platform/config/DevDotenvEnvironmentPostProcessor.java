package me.neobliz1.ecomonitoring.platform.config;

import lombok.NonNull;
import me.neobliz1.ecomonitoring.platform.model.exception.DotenvLoadException;
import org.springframework.boot.EnvironmentPostProcessor;
import org.springframework.boot.SpringApplication;
import org.springframework.core.Ordered;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.Profiles;
import org.springframework.core.env.PropertiesPropertySource;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class DevDotenvEnvironmentPostProcessor implements EnvironmentPostProcessor, Ordered {

    @Override
    public void postProcessEnvironment(@NonNull ConfigurableEnvironment environment, @NonNull SpringApplication application) {
        if(environment.acceptsProfiles(Profiles.of("dev"))) {
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            if(classLoader==null) {
                classLoader = DevDotenvEnvironmentPostProcessor.class.getClassLoader();
            }
            try(InputStream inputStream = classLoader.getResourceAsStream(".env")) {
                if(inputStream==null) {
                    return;
                }
                Properties dotenvProps = new Properties();
                dotenvProps.load(inputStream);
                environment.getPropertySources().addFirst(
                        new PropertiesPropertySource("envProps", dotenvProps)
                );
            } catch(IOException e) {
                throw new DotenvLoadException(e);
            }
        }
    }

    @Override
    public int getOrder() {
        return Ordered.LOWEST_PRECEDENCE;
    }
}

