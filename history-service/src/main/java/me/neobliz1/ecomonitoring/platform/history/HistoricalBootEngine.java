package me.neobliz1.ecomonitoring.platform.history;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = {
        "me.neobliz1.ecomonitoring.platform.history",
        "me.neobliz1.ecomonitoring.platform.common.config"
})
public class HistoricalBootEngine {

    public static void main(String[] args) {
        SpringApplication.run(HistoricalBootEngine.class);
    }
}
