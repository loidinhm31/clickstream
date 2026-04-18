package com.clickstream.controller;

import com.clickstream.model.ClickEvent;
import com.clickstream.service.EventPublisher;
import com.clickstream.validation.EventValidator;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * REST Controller for click event ingestion.
 * 
 * Endpoints:
 * - POST /api/events - Ingest single event
 * - POST /api/events/batch - Ingest multiple events (up to 100)
 * 
 * Response: 202 Accepted (fire-and-forget async Kafka send)
 * Errors: 400 Bad Request (validation failures)
 */
@RestController
@RequestMapping("/api/events")
@CrossOrigin(origins = "${clickstream.cors.allowed-origins:http://localhost:3000}")
public class EventController {

    private static final Logger logger = LoggerFactory.getLogger(EventController.class);

    private final EventPublisher eventPublisher;
    private final EventValidator eventValidator;
    private final int maxBatchSize;

    public EventController(
            EventPublisher eventPublisher,
            EventValidator eventValidator,
            @Value("${clickstream.batch.max-size:100}") int maxBatchSize) {
        this.eventPublisher = eventPublisher;
        this.eventValidator = eventValidator;
        this.maxBatchSize = maxBatchSize;
    }

    /**
     * Ingest single click event.
     * 
     * @param event ClickEvent from frontend (sendBeacon)
     * @return 202 Accepted if validation passes, 400 if validation fails
     */
    @PostMapping
    public ResponseEntity<IngestionResponse> ingest(@Valid @RequestBody ClickEvent event) {
        logger.debug("Received single event: type={}, sessionId={}", 
                event.getEventType(), event.getSessionId());
        
        // Additional business validation beyond @Valid annotations
        List<String> errors = eventValidator.validate(event);
        if (!errors.isEmpty()) {
            logger.warn("Event validation failed: {}", errors);
            return ResponseEntity.badRequest()
                    .body(new IngestionResponse(false, "Validation failed", errors));
        }
        
        // Publish async to Kafka (fire-and-forget)
        eventPublisher.publishAsync(event);
        
        return ResponseEntity.accepted()
                .body(new IngestionResponse(true, "Event accepted", null));
    }

    /**
     * Ingest batch of click events.
     * 
     * @param events List of ClickEvents (max 100)
     * @return 202 Accepted if all valid, 400 if validation fails or batch too large
     */
    @PostMapping("/batch")
    public ResponseEntity<IngestionResponse> ingestBatch(@RequestBody List<@Valid ClickEvent> events) {
        logger.info("Received batch of {} events", events.size());
        
        // Check batch size limit
        if (events.size() > maxBatchSize) {
            logger.warn("Batch size exceeds limit: {} > {}", events.size(), maxBatchSize);
            return ResponseEntity.badRequest()
                    .body(new IngestionResponse(false, 
                            String.format("Batch size exceeds limit of %d", maxBatchSize), 
                            null));
        }
        
        // Validate all events (fail fast)
        for (int i = 0; i < events.size(); i++) {
            ClickEvent event = events.get(i);
            List<String> errors = eventValidator.validate(event);
            if (!errors.isEmpty()) {
                logger.warn("Event validation failed at index {}: {}", i, errors);
                return ResponseEntity.badRequest()
                        .body(new IngestionResponse(false, 
                                String.format("Event at index %d failed validation", i), 
                                errors));
            }
        }
        
        // Publish all events async
        eventPublisher.publishBatch(events);
        
        return ResponseEntity.accepted()
                .body(new IngestionResponse(true, 
                        String.format("Batch of %d events accepted", events.size()), 
                        null));
    }

    /**
     * Health check endpoint
     */
    @GetMapping("/health")
    public ResponseEntity<String> health() {
        return ResponseEntity.ok("Ingestion API is healthy");
    }

    /**
     * Response DTO for ingestion endpoints
     */
    public static class IngestionResponse {
        private boolean success;
        private String message;
        private List<String> errors;

        public IngestionResponse(boolean success, String message, List<String> errors) {
            this.success = success;
            this.message = message;
            this.errors = errors;
        }

        public boolean isSuccess() { return success; }
        public void setSuccess(boolean success) { this.success = success; }

        public String getMessage() { return message; }
        public void setMessage(String message) { this.message = message; }

        public List<String> getErrors() { return errors; }
        public void setErrors(List<String> errors) { this.errors = errors; }
    }
}
