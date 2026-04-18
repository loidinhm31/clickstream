package com.clickstream;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Clickstream Ingestion API
 * 
 * Dual-purpose Spring Boot application:
 * 1. Ingestion: Receive click events via REST → publish to Kafka
 * 2. Analytics: Serve historical session data from MongoDB
 * 
 * Port: 8081 (8080 reserved for Kafka UI)
 */
@SpringBootApplication
public class ClickstreamApplication {

    public static void main(String[] args) {
        SpringApplication.run(ClickstreamApplication.class, args);
    }
}
