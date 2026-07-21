package me.neobliz1.ecomonitoring.platform.analysis;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan(basePackages = "me.neobliz1.ecomonitoring.platform")
public class AnalysisBootEngine {

    public static void main(String[] args) {
        SpringApplication.run(AnalysisBootEngine.class, args);
    }
}
