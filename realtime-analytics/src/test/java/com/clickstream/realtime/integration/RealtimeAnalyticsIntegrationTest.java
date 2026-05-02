package com.clickstream.realtime.integration;

import com.clickstream.model.ClickEvent;
import com.clickstream.model.EventType;
import com.clickstream.realtime.engine.MetricsEngine;
import com.clickstream.realtime.engine.ArrowMetricsSnapshot;
import com.clickstream.realtime.kafka.EventConsumer;
import com.clickstream.realtime.serialization.ArrowIPCSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.awaitility.Awaitility.await;

/**
 * Integration test for real-time analytics pipeline.
 * 
 * <p>Tests the full flow:
 * 1. Produce events to Kafka
 * 2. EventConsumer consumes events
 * 3. MetricsEngine processes events
 * 4. Metrics are computed correctly
 * 5. ArrowIPCSerializer serializes metrics
 * 
 * <p>Uses embedded Kafka for isolated testing.
 */
@SpringBootTest
@DirtiesContext
@EmbeddedKafka(
        partitions = 1,
        topics = {"clickstream-events"}
)
@TestPropertySource(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.consumer.auto-offset-reset=earliest",
        "spring.kafka.listener.ack-mode=batch",
        "metrics.websocket.allowed-origins=http://localhost:3000"
})
@DisplayName("Realtime Analytics Integration Tests")
class RealtimeAnalyticsIntegrationTest {

    @Autowired
    private MetricsEngine metricsEngine;

    @Autowired
    private ArrowIPCSerializer serializer;

    @Autowired
    private EventConsumer eventConsumer;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;

    private KafkaTemplate<String, ClickEvent> kafkaTemplate;

    @BeforeEach
    void setUp() {
        metricsEngine.reset();
        
        // Create Kafka producer for test
        Map<String, Object> producerProps = KafkaTestUtils.producerProps(embeddedKafka);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        kafkaTemplate = new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(producerProps));
    }

    @Test
    @DisplayName("Should process events from Kafka and compute metrics")
    void testEndToEndFlow() {
        // Produce test events to Kafka
        for (int i = 0; i < 10; i++) {
            ClickEvent event = createTestEvent("user" + i, "sess" + i, EventType.PAGE_VIEW, "/home");
            kafkaTemplate.send(new ProducerRecord<>("clickstream-events", event.getSessionId(), event));
        }

        // Wait for consumer to process events
        await()
                .atMost(10, TimeUnit.SECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .until(() -> {
                    ArrowMetricsSnapshot metrics = metricsEngine.computeMetrics();
                    return metrics.activeUsers() > 0;
                });

        // Verify metrics
        ArrowMetricsSnapshot metrics = metricsEngine.computeMetrics();
        assertTrue(metrics.activeUsers() > 0);
        assertTrue(metrics.eventRate() > 0);
    }

    @Test
    @DisplayName("Should handle CLICK events correctly")
    void testClickEventsProcessing() {
        // Produce CLICK events
        for (int i = 0; i < 5; i++) {
            ClickEvent event = createTestEvent("user1", "sess1", EventType.CLICK, "/button");
            kafkaTemplate.send(new ProducerRecord<>("clickstream-events", event.getSessionId(), event));
        }

        // Wait for processing
        await()
                .atMost(10, TimeUnit.SECONDS)
                .until(() -> {
                    ArrowMetricsSnapshot metrics = metricsEngine.computeMetrics();
                    return metrics.clicksPerSecond() > 0;
                });

        ArrowMetricsSnapshot metrics = metricsEngine.computeMetrics();
        assertTrue(metrics.clicksPerSecond() > 0);
    }

    @Test
    @DisplayName("Should compute trending pages from Kafka events")
    void testTrendingPagesFromKafka() {
        // Produce events to different pages
        String[] pages = {"/home", "/home", "/home", "/about", "/about", "/contact"};
        
        for (String page : pages) {
            ClickEvent event = createTestEvent("user1", "sess1", EventType.PAGE_VIEW, page);
            kafkaTemplate.send(new ProducerRecord<>("clickstream-events", event.getSessionId(), event));
        }

        // Wait for processing
        await()
                .atMost(10, TimeUnit.SECONDS)
                .until(() -> {
                    ArrowMetricsSnapshot metrics = metricsEngine.computeMetrics();
                    return !metrics.trendingPages().isEmpty();
                });

        ArrowMetricsSnapshot metrics = metricsEngine.computeMetrics();
        assertFalse(metrics.trendingPages().isEmpty());
        
        // /home should be top with 3 views
        assertEquals("/home", metrics.trendingPages().get(0).pageUrl());
        assertEquals(3, metrics.trendingPages().get(0).viewCount());
    }

    @Test
    @DisplayName("Should serialize metrics to Arrow IPC after Kafka processing")
    void testArrowSerializationAfterKafka() throws Exception {
        // Produce events
        for (int i = 0; i < 5; i++) {
            ClickEvent event = createTestEvent("user" + i, "sess" + i, EventType.PAGE_VIEW, "/test");
            kafkaTemplate.send(new ProducerRecord<>("clickstream-events", event.getSessionId(), event));
        }

        // Wait for processing
        await()
                .atMost(10, TimeUnit.SECONDS)
                .until(() -> {
                    ArrowMetricsSnapshot metrics = metricsEngine.computeMetrics();
                    return metrics.activeUsers() > 0;
                });

        // Serialize to Arrow IPC
        ArrowMetricsSnapshot metrics = metricsEngine.computeMetrics();
        byte[] arrowIPC = serializer.serializeToArrowIPC(metrics);

        assertNotNull(arrowIPC);
        assertTrue(arrowIPC.length > 0);
    }

    @Test
    @DisplayName("Should handle high-volume event stream")
    void testHighVolumeStream() {
        // Produce 100 events rapidly
        for (int i = 0; i < 100; i++) {
            ClickEvent event = createTestEvent("user" + (i % 20), "sess" + i, EventType.PAGE_VIEW, "/page" + (i % 10));
            kafkaTemplate.send(new ProducerRecord<>("clickstream-events", event.getSessionId(), event));
        }

        // Wait for processing
        await()
                .atMost(15, TimeUnit.SECONDS)
                .until(() -> {
                    ArrowMetricsSnapshot metrics = metricsEngine.computeMetrics();
                    return metrics.eventRate() > 5; // Should have high event rate
                });

        ArrowMetricsSnapshot metrics = metricsEngine.computeMetrics();
        assertTrue(metrics.activeUsers() > 0);
        assertTrue(metrics.eventRate() > 5);
        assertFalse(metrics.trendingPages().isEmpty());
    }

    // Helper method
    private ClickEvent createTestEvent(String userId, String sessionId, EventType type, String pageUrl) {
        com.clickstream.model.EventMetadata.Builder metadataBuilder = com.clickstream.model.EventMetadata.builder()
                .viewportWidth(1920)
                .viewportHeight(1080);
        
        if (type == EventType.CLICK) {
            metadataBuilder.x(100).y(200);
        } else if (type == EventType.SCROLL) {
            metadataBuilder.scrollDepth(0.5);
        }

        return ClickEvent.builder()
                .eventId("evt-" + System.nanoTime())
                .userId(userId)
                .sessionId(sessionId)
                .eventType(type)
                .targetElement(type == EventType.CLICK ? "button#test" : null)
                .pageUrl(pageUrl)
                .referrerUrl(null)
                .timestamp(System.currentTimeMillis())
                .userAgent("Test-Agent/1.0")
                .metadata(metadataBuilder.build())
                .build();
    }
}
