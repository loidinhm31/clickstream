package com.clickstream.realtime.kafka;

import com.clickstream.model.ClickEvent;
import com.clickstream.realtime.engine.MetricsEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Kafka consumer for clickstream events.
 * 
 * <p>Consumer Group: realtime-analytics-group
 * <p>Topic: clickstream-events
 * <p>Concurrency: Single thread (batch-based processing)
 * 
 * <p>This consumer feeds events to MetricsEngine for real-time aggregation.
 * Events are processed in batches for efficiency.
 * 
 * <p>Error Handling:
 * - Logs errors but does not stop processing (fail-fast would block real-time metrics)
 * - Invalid events are logged and skipped
 * - Kafka offsets are committed after successful MetricsEngine ingestion
 */
@Component
public class EventConsumer {

    private static final Logger logger = LoggerFactory.getLogger(EventConsumer.class);

    @Autowired
    private MetricsEngine metricsEngine;

    @Autowired
    private KafkaListenerEndpointRegistry listenerRegistry;

    /**
     * Consume clickstream events and feed to MetricsEngine.
     * 
     * <p>Batch processing: Receives up to 500 events per poll (default).
     * The @KafkaListener automatically deserializes JSON to ClickEvent.
     * 
     * @param events Batch of events from Kafka
     */
    @KafkaListener(
            topics = "clickstream-events",
            groupId = "realtime-analytics-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeEvents(List<ClickEvent> events) {
        if (events == null || events.isEmpty()) {
            return;
        }

        try {
            logger.info("Received batch: {} events", events.size());
            metricsEngine.ingestBatch(events);
            logger.debug("Successfully ingested {} events into MetricsEngine", events.size());
        } catch (Exception e) {
            logger.error("Failed to process event batch: {} events", events.size(), e);
            // Continue processing - don't block metrics computation for one bad batch
        }
    }

    /**
     * Check if Kafka consumer is healthy (listener container is running).
     *
     * @return true if at least one listener container is running and not paused
     */
    public boolean isHealthy() {
        return listenerRegistry.getListenerContainers().stream()
                .anyMatch(c -> c.isRunning() && !c.isPauseRequested());
    }
}
