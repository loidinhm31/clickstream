package com.clickstream.service;

import com.clickstream.model.ClickEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

/**
 * Event publisher service - wraps KafkaTemplate for async event publishing.
 * 
 * Key features:
 * - Async send with CompletableFuture
 * - Partition by sessionId (preserves session ordering)
 * - JSON serialization of events
 * - Error logging (does not block ingestion on Kafka failures)
 */
@Service
public class EventPublisher {

    private static final Logger logger = LoggerFactory.getLogger(EventPublisher.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final String topic;

    public EventPublisher(
            KafkaTemplate<String, String> kafkaTemplate,
            ObjectMapper objectMapper,
            @Value("${clickstream.kafka.topic}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.topic = topic;
    }

    /**
     * Publish event asynchronously to Kafka.
     * 
     * @param event ClickEvent to publish
     * @return CompletableFuture that completes when send succeeds or fails
     */
    public CompletableFuture<SendResult<String, String>> publishAsync(ClickEvent event) {
        try {
            // Use sessionId as partition key - ensures all events from same session go to same partition
            String key = event.getSessionId();
            String value = objectMapper.writeValueAsString(event);
            
            logger.debug("Publishing event: sessionId={}, type={}, eventId={}", 
                    key, event.getEventType(), event.getEventId());
            
            CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(topic, key, value);
            
            future.whenComplete((result, ex) -> {
                if (ex != null) {
                    logger.error("Failed to send event to Kafka: sessionId={}, eventId={}", 
                            key, event.getEventId(), ex);
                } else {
                    logger.debug("Event sent successfully: partition={}, offset={}", 
                            result.getRecordMetadata().partition(), 
                            result.getRecordMetadata().offset());
                }
            });
            
            return future;
            
        } catch (JsonProcessingException e) {
            logger.error("Failed to serialize event: eventId={}", 
                    event.getEventId(), e);
            return CompletableFuture.failedFuture(e);
        }
    }

    /**
     * Publish multiple events in batch (internally calls publishAsync for each).
     * 
     * @param events List of events to publish
     * @return CompletableFuture that completes when all sends complete
     */
    public CompletableFuture<Void> publishBatch(java.util.List<ClickEvent> events) {
        logger.info("Publishing batch of {} events", events.size());
        
        CompletableFuture<?>[] futures = events.stream()
                .map(this::publishAsync)
                .toArray(CompletableFuture[]::new);
        
        return CompletableFuture.allOf(futures);
    }
}
