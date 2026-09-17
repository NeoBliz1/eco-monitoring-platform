package me.neobliz1.ecomonitoring.platform.ingestion;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = {
        "me.neobliz1.ecomonitoring.platform.ingestion",
        "me.neobliz1.ecomonitoring.platform.common.config"
})
public class IngestionBootEngine {

    public static void main(String[] args) {
        SpringApplication.run(IngestionBootEngine.class, args);
    }
}
