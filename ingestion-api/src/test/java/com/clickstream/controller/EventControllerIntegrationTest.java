package com.clickstream.controller;

import com.clickstream.model.ClickEvent;
import com.clickstream.model.EventType;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.web.servlet.MockMvc;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/**
 * Integration tests for EventController.
 * Uses embedded Kafka to verify events are published correctly.
 */
@SpringBootTest
@AutoConfigureMockMvc
@EmbeddedKafka(
        topics = {"clickstream-events"},
        partitions = 3,
        brokerProperties = {
                "listeners=PLAINTEXT://localhost:9093",
                "port=9093"
        }
)
class EventControllerIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;

    private KafkaConsumer<String, String> consumer;

    @BeforeEach
    void setUp() {
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(
                "test-consumer-group", "true", embeddedKafka);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList("clickstream-events"));
    }

    @AfterEach
    void tearDown() {
        if (consumer != null) {
            consumer.close();
        }
    }

    @Test
    void testIngestSingleEvent_ReturnsAccepted() throws Exception {
        // Given - valid click event
        ClickEvent event = ClickEvent.builder()
                .eventId(UUID.randomUUID().toString())
                .userId("user-123")
                .sessionId("session-456")
                .eventType(EventType.CLICK)
                .targetElement("button#submit")
                .pageUrl("https://example.com/checkout")
                .referrerUrl("https://example.com/cart")
                .timestamp(System.currentTimeMillis())
                .userAgent("Mozilla/5.0")
                .build();

        String eventJson = objectMapper.writeValueAsString(event);

        // When - POST event
        mockMvc.perform(post("/api/events")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(eventJson))
                // Then - 202 Accepted
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.message").value("Event accepted"));

        // Verify event published to Kafka
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
        assertThat(records).isNotEmpty();
        
        ConsumerRecord<String, String> record = records.iterator().next();
        assertThat(record.key()).isEqualTo(event.getSessionId());
        
        ClickEvent publishedEvent = objectMapper.readValue(record.value(), ClickEvent.class);
        assertThat(publishedEvent.getEventId()).isEqualTo(event.getEventId());
        assertThat(publishedEvent.getEventType()).isEqualTo(EventType.CLICK);
    }

    @Test
    void testIngestBatch_ReturnsAccepted() throws Exception {
        // Given - batch of 3 events
        String sessionId = "session-789";
        ClickEvent event1 = createTestEvent(sessionId, EventType.PAGE_VIEW);
        ClickEvent event2 = createTestEvent(sessionId, EventType.CLICK);
        ClickEvent event3 = createTestEvent(sessionId, EventType.SCROLL);

        String batchJson = objectMapper.writeValueAsString(
                java.util.List.of(event1, event2, event3));

        // When - POST batch
        mockMvc.perform(post("/api/events/batch")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(batchJson))
                // Then - 202 Accepted
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.message").value("Batch of 3 events accepted"));

        // Verify all events published to Kafka
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
        assertThat(records.count()).isEqualTo(3);
    }

    @Test
    void testIngestInvalidEvent_ReturnsBadRequest() throws Exception {
        // Given - invalid event (missing required fields)
        String invalidJson = "{\"eventType\":\"CLICK\"}";

        // When - POST invalid event
        mockMvc.perform(post("/api/events")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(invalidJson))
                // Then - 400 Bad Request
                .andExpect(status().isBadRequest());
    }

    @Test
    void testIngestBatchTooLarge_ReturnsBadRequest() throws Exception {
        // Given - batch exceeding max size (101 events)
        java.util.List<ClickEvent> largeBatch = new java.util.ArrayList<>();
        for (int i = 0; i < 101; i++) {
            largeBatch.add(createTestEvent("session-large", EventType.CLICK));
        }

        String batchJson = objectMapper.writeValueAsString(largeBatch);

        // When - POST large batch
        mockMvc.perform(post("/api/events/batch")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(batchJson))
                // Then - 400 Bad Request
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.success").value(false))
                .andExpect(jsonPath("$.message").value("Batch size exceeds limit of 100"));
    }

    @Test
    void testHealthEndpoint_ReturnsOk() throws Exception {
        mockMvc.perform(get("/api/events/health"))
                .andExpect(status().isOk())
                .andExpect(content().string("Ingestion API is healthy"));
    }

    @Test
    void testCorsHeaders_Present() throws Exception {
        mockMvc.perform(get("/api/events/health")
                        .header("Origin", "http://localhost:3000"))
                .andExpect(status().isOk())
                .andExpect(header().exists("Access-Control-Allow-Origin"));
    }

    // Helper methods

    private ClickEvent createTestEvent(String sessionId, EventType eventType) {
        return ClickEvent.builder()
                .eventId(UUID.randomUUID().toString())
                .userId("user-test")
                .sessionId(sessionId)
                .eventType(eventType)
                .targetElement("div#test")
                .pageUrl("https://example.com/test")
                .referrerUrl("https://example.com/home")
                .timestamp(System.currentTimeMillis())
                .userAgent("Test Agent")
                .build();
    }
}
