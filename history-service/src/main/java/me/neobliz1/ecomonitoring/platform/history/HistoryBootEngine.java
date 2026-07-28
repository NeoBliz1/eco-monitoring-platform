package me.neobliz1.ecomonitoring.platform.history;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan(basePackages = "me.neobliz1.ecomonitoring.platform")
public class HistoryBootEngine {

    public static void main(String[] args) {
        SpringApplication.run(HistoryBootEngine.class);
    }
}
