package com.clickstream.etl;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Main application class for Spark ETL service.
 * 
 * <p>This application runs a Spark Structured Streaming job that:
 * <ul>
 *   <li>Consumes clickstream events from Kafka topic 'clickstream-events'</li>
 *   <li>Processes events through three parallel streams:</li>
 *   <ul>
 *     <li>Session aggregation (30-min session windows)</li>
 *     <li>Page-level metrics (5-min tumbling windows)</li>
 *     <li>User journey tracking (session-based)</li>
 *   </ul>
 *   <li>Writes results to MongoDB collections via foreachBatch sink</li>
 * </ul>
 * 
 * <p>Architecture:
 * <pre>
 * Kafka → Spark readStream → [Session | Page | Journey] → MongoDB
 * </pre>
 * 
 * <p>JVM Requirements:
 * Must run with: --add-exports java.base/sun.nio.ch=ALL-UNNAMED
 * (Spark internal buffer access requirement)
 */
@SpringBootApplication
public class SparkETLApplication {

    public static void main(String[] args) {
        SpringApplication.run(SparkETLApplication.class, args);
    }
}
