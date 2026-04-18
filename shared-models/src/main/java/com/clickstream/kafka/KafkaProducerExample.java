package com.clickstream.kafka;

import com.clickstream.model.ClickEvent;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Example Kafka producer utility demonstrating event publishing patterns.
 * 
 * <p>This is a reference implementation showing how to:
 * <ul>
 *   <li>Use sessionId as the partition key (preserves session ordering)</li>
 *   <li>Serialize events to JSON before publishing</li>
 *   <li>Structure ProducerRecord for the clickstream-events topic</li>
 * </ul>
 * 
 * <p><strong>Note:</strong> This is a code example only. Actual Kafka integration
 * will be implemented in Phase 3 (Spring Boot ingestion API) using KafkaTemplate.
 * 
 * <p>For production use:
 * <pre>
 * // Phase 3 will use Spring Kafka:
 * {@code @Autowired}
 * private KafkaTemplate&lt;String, String&gt; kafkaTemplate;
 * 
 * public void publish(ClickEvent event) {
 *     String key = event.getSessionId();  // partition by sessionId
 *     String value = objectMapper.writeValueAsString(event);
 *     kafkaTemplate.send("clickstream-events", key, value);
 * }
 * </pre>
 */
public class KafkaProducerExample {

    private static final String TOPIC = "clickstream-events";
    private final ObjectMapper objectMapper;

    public KafkaProducerExample() {
        this.objectMapper = new ObjectMapper();
    }

    /**
     * Demonstrates how to construct a Kafka ProducerRecord for a ClickEvent.
     * 
     * <p><strong>Key design decision:</strong> Use sessionId (not userId) as partition key:
     * <ul>
     *   <li>Distributes load evenly (sessions are time-bounded, avoid hot partitions)</li>
     *   <li>Preserves event ordering within a session (all events → same partition)</li>
     *   <li>Enables efficient Spark session windowing (no cross-partition joins)</li>
     * </ul>
     * 
     * @param event the clickstream event to publish
     * @return example ProducerRecord structure (pseudo-code)
     */
    public String createProducerRecordExample(ClickEvent event) {
        try {
            String key = event.getSessionId();  // CRITICAL: sessionId as partition key
            String value = objectMapper.writeValueAsString(event);
            
            // In Phase 3, this will be:
            // ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, key, value);
            // return kafkaTemplate.send(record);
            
            return String.format(
                "ProducerRecord<String, String>(\n" +
                "  topic: '%s',\n" +
                "  key: '%s',  // sessionId for partition affinity\n" +
                "  value: %s\n" +
                ")",
                TOPIC,
                key,
                value
            );
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize ClickEvent", e);
        }
    }

    /**
     * Explains partition key selection rationale.
     * 
     * @return multi-line explanation string
     */
    public String explainPartitioningStrategy() {
        return """
            ---
            Partitioning Strategy: sessionId
            ---
            
            Why sessionId over userId?
            - userId creates hot partitions (power users generate 10-100x more events)
            - sessionId naturally bounded in time (~30 min), distributes evenly
            - All events in a session → same partition → preserves chronological order
            - Spark session window aggregation works on single partition (no shuffle)
            
            Why not random (null key)?
            - Loses ordering guarantees
            - Events from same session scattered across partitions
            - Session reconstruction requires expensive cross-partition joins
            
            Kafka Configuration:
            - Topic: clickstream-events
            - Partitions: 6 (matches CPU cores for dev)
            - Replication Factor: 1 (dev), 3 (prod)
            - Retention: 7 days
            - Compression: lz4 (fast, high throughput)
            
            Producer Configuration (application.yml):
            spring:
              kafka:
                bootstrap-servers: localhost:9092
                producer:
                  key-serializer: org.apache.kafka.common.serialization.StringSerializer
                  value-serializer: org.apache.kafka.common.serialization.StringSerializer
                  acks: 1               # leader ack (fast); use 'all' for prod
                  compression-type: lz4
                  batch-size: 16384     # 16KB batches
                  linger-ms: 5          # 5ms batch window (low latency)
            """;
    }

    /**
     * Example method showing complete publish flow (pseudo-code).
     * Phase 3 will implement this with actual KafkaTemplate.
     */
    public void publishEventExample(ClickEvent event) {
        // 1. Validate event before publishing
        // EventValidator validator = new EventValidator();
        // List<String> errors = validator.validate(event);
        // if (!errors.isEmpty()) {
        //     throw new ValidationException("Invalid event: " + errors);
        // }

        // 2. Serialize to JSON
        // String json = objectMapper.writeValueAsString(event);

        // 3. Publish with sessionId as key
        // ProducerRecord<String, String> record = new ProducerRecord<>(
        //     "clickstream-events",
        //     event.getSessionId(),  // key
        //     json                    // value
        // );

        // 4. Send async (non-blocking)
        // kafkaTemplate.send(record)
        //     .addCallback(
        //         result -> log.debug("Event published: {}", event.getEventId()),
        //         ex -> log.error("Publish failed: {}", event.getEventId(), ex)
        //     );
    }
}
