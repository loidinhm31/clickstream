package com.clickstream.realtime;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Real-time Analytics Service - Main Application.
 * 
 * <p>This service consumes Kafka events, builds Apache Arrow columnar tables in-memory,
 * and serves real-time metrics via WebSocket (push) and HTTP (pull).
 * 
 * <p>Architecture:
 * <ul>
 *   <li>Kafka Consumer: Ingests events from clickstream-events topic</li>
 *   <li>MetricsEngine: Maintains ring buffer of Arrow VectorSchemaRoot batches</li>
 *   <li>WebSocket Handler: Pushes Arrow IPC frames every 1.5s to connected clients</li>
 *   <li>HTTP Controller: Provides on-demand metrics via GET endpoint</li>
 * </ul>
 * 
 * <p>Memory Management: Uses Apache Arrow off-heap buffers with automatic eviction
 * of old batches beyond 15-minute sliding window.
 */
@SpringBootApplication
@EnableScheduling
public class RealtimeApplication {

    public static void main(String[] args) {
        SpringApplication.run(RealtimeApplication.class, args);
    }
}
